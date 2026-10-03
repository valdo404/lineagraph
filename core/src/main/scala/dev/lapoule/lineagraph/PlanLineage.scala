package dev.lapoule.lineagraph

import dev.lapoule.lineagraph.model._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, DataWritingCommand}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InsertIntoHadoopFsRelationCommand, LogicalRelation, SaveIntoDataSourceCommand}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

/**
 * Extracts dataset- and column-level lineage from an analyzed Spark logical plan.
 *
 * Column lineage is computed bottom-up over expression ids: every attribute produced
 * by a node is mapped to the source fields it is derived from, with the OpenLineage
 * transformation that links them.
 */
object PlanLineage {

  val OutputNamespace = "lineagraph"

  /** Lineage of a DataFrame: the DataFrame itself is the (virtual) output dataset. */
  def extract(df: DataFrame, source: SourceInfo = SourceInfo.unknown, labels: Map[String, LogicalPlan] = Map.empty): LineageGraph = {
    // Named after the val holding it, else after the enclosing Class.method.
    val name   = source.valName.getOrElse(source.owner.split('.').takeRight(2).mkString("."))
    val output = DatasetNode(OutputNamespace, name, fields(df.schema))
    fromPlan(df.queryExecution.analyzed, output, source, labels)
  }

  /** Lineage of a write command (as seen by a QueryExecutionListener), if `plan` is one. */
  def extractWrite(plan: LogicalPlan, source: SourceInfo): Option[LineageGraph] =
    writeTarget(plan).map { case (query, target, outputNames) =>
      val renamed = Project(query.output.zip(outputNames).map { case (a, n) => Alias(a, n)() }, query)
      fromPlan(renamed, target.copy(fields = fields(renamed.schema)), source, Map.empty)
    }

  def fromPlan(plan: LogicalPlan, output: DatasetNode, source: SourceInfo, labels: Map[String, LogicalPlan]): LineageGraph =
    new Extractor(labels).run(plan, output, source)

  private def fields(schema: StructType): Seq[Field] = schema.fields.toSeq.map(f => Field(f.name, f.dataType.simpleString))

  private def tableNode(t: CatalogTable, schema: StructType): DatasetNode = {
    val id = t.identifier
    DatasetNode(id.catalog.getOrElse("spark_catalog"), (id.database.toSeq :+ id.table).mkString("."), fields(schema))
  }

  private def writeTarget(plan: LogicalPlan): Option[(LogicalPlan, DatasetNode, Seq[String])] = plan match {
    case c: InsertIntoHadoopFsRelationCommand =>
      val node = c.catalogTable.map(tableNode(_, c.query.schema))
        .getOrElse(DatasetNode("file", c.outputPath.toUri.getPath, Nil))
      Some((c.query, node, c.outputColumnNames))
    case c: CreateDataSourceTableAsSelectCommand =>
      Some((c.query, tableNode(c.table, c.query.schema), c.outputColumnNames))
    case c: DataWritingCommand =>
      Some((c.query, DatasetNode("spark", c.nodeName, Nil), c.outputColumnNames))
    case c: SaveIntoDataSourceCommand =>
      val path = c.options.get("path").orElse(c.options.get("dbtable")).getOrElse(c.dataSource.getClass.getSimpleName)
      Some((c.query, DatasetNode(c.dataSource.getClass.getSimpleName.stripSuffix("$").toLowerCase, path, Nil), c.query.output.map(_.name)))
    case c: V2WriteCommand =>
      Some((c.query, DatasetNode("v2", c.table.name, Nil), c.query.output.map(_.name)))
    case _ => None
  }

  private final class Extractor(labels: Map[String, LogicalPlan]) {
    private val deps        = mutable.Map.empty[ExprId, Set[InputDep]]
    private val datasetDeps = mutable.LinkedHashSet.empty[InputDep]
    private val inputs      = mutable.LinkedHashMap.empty[String, DatasetNode]
    private val dsLabels    = mutable.LinkedHashMap.empty[String, String]
    private val operations  = mutable.ArrayBuffer.empty[OperationNode]
    private val edges       = mutable.LinkedHashSet.empty[Edge]
    private val cteDefs     = mutable.Map.empty[Long, CTERelationDef]
    // Identity-keyed: two structurally equal subtrees (e.g. a self join) are distinct nodes.
    private val visited     = new java.util.IdentityHashMap[LogicalPlan, Option[String]]()
    private var counter     = 0

    def run(plan: LogicalPlan, output: DatasetNode, source: SourceInfo): LineageGraph = {
      val root = visit(plan)
      root.foreach(r => edges += Edge(r, output.id))
      val columns = plan.output.map(a => ColumnLineage(a.name, depsOf(a).toSeq.sortBy(_.field.toString)))
      LineageGraph(
        source = source,
        inputs = inputs.values.toSeq,
        output = output,
        operations = operations.toSeq,
        edges = edges.toSeq,
        columns = columns,
        datasetDeps = datasetDeps.toSeq,
        labels = dsLabels.toMap
      )
    }

    private def depsOf(a: Attribute): Set[InputDep] = deps.getOrElse(a.exprId, Set.empty)

    /** Source fields an expression reads, each composed with the expression's own effect. */
    private def resolve(e: Expression): Set[InputDep] = {
      val effect = e match {
        case _: Attribute                                                  => Transformation.identity
        case Alias(_: Attribute, _)                                        => Transformation.identity
        case _ if e.exists(_.isInstanceOf[AggregateExpression])            => Transformation.aggregation
        case _                                                             => Transformation.transformation
      }
      // PARTITION BY / ORDER BY of a window only shape the rows: they are recorded as INDIRECT on the Window node.
      val valueOnly      = e.transform { case w: WindowExpression => w.windowFunction }
      val fromAttributes = valueOnly.references.toSeq.flatMap(depsOf)
      val fromSubqueries = e.collect { case s: SubqueryExpression => s.plan }.flatMap { p =>
        visitSubquery(p)
        p.output.flatMap(depsOf)
      }
      (fromAttributes ++ fromSubqueries).map(d => d.copy(transformation = Transformation.compose(d.transformation, effect))).toSet
    }

    private def indirect(subtype: TransformationSubtype, exprs: Seq[Expression]): Unit =
      exprs.flatMap(e => e.references.toSeq.flatMap(depsOf)).foreach { d =>
        datasetDeps += InputDep(d.field, Transformation.indirect(subtype))
      }

    private def visitSubquery(p: LogicalPlan): Unit = { visit(p); () }

    /** Returns the graph node id representing `plan` (None for transparent nodes without input). */
    private def visit(plan: LogicalPlan): Option[String] =
      if (visited.containsKey(plan)) visited.get(plan)
      else {
        val id = doVisit(plan)
        visited.put(plan, id)
        id
      }

    private def doVisit(plan: LogicalPlan): Option[String] = plan match {
      case w: WithCTE =>
        w.cteDefs.foreach(d => cteDefs.update(d.id, d))
        w.cteDefs.foreach(visit)
        visit(w.plan)

      case d: CTERelationDef => visit(d.child)

      case r: CTERelationRef =>
        cteDefs.get(r.cteId).flatMap { d =>
          val child = visit(d)
          r.output.zip(d.output).foreach { case (o, i) => deps.update(o.exprId, depsOf(i)) }
          child
        }

      case _: OneRowRelation => None

      // A temp view over in-memory data (Seq.toDF, createDataFrame...) is the dataset itself:
      // its columns are the view's, not the `_1`, `_2` of the underlying LocalRelation.
      case s: SubqueryAlias if isInMemory(s.child) =>
        source(s, DatasetNode("memory", s.alias, fields(s.schema)))

      case leaf: LeafNode => source(leaf, leafDataset(leaf))

      // Pure renaming nodes: transparent for both graphs.
      case s: SubqueryAlias =>
        val child = visit(s.child)
        s.output.zip(s.child.output).foreach { case (o, i) => if (o.exprId != i.exprId) deps.update(o.exprId, depsOf(i)) }
        child

      case u: Union =>
        val children = u.children.flatMap(visit)
        u.output.indices.foreach { i =>
          deps.update(u.output(i).exprId, u.children.flatMap(c => depsOf(c.output(i))).toSet)
        }
        Some(operation(u, children, "Union", s"${u.children.size} branches"))

      case other =>
        val children = other.children.flatMap(visit)
        // Every alias introduced by this node: Project, Aggregate, Window, Generate...
        other.expressions.foreach(_.foreach {
          case a: Alias => deps.update(a.exprId, resolve(a.child))
          case _        =>
        })
        other match {
          case j: Join      => indirect(TransformationSubtype.Join, j.condition.toSeq)
          case f: Filter    => indirect(TransformationSubtype.Filter, Seq(f.condition))
          case a: Aggregate => indirect(TransformationSubtype.GroupBy, a.groupingExpressions)
          case s: Sort      => indirect(TransformationSubtype.Sort, s.order)
          case w: Window    => indirect(TransformationSubtype.Window, w.partitionSpec ++ w.orderSpec)
          case _            =>
        }
        // Attributes produced without an alias (Generate, Expand...): depend on everything referenced.
        other.output.filterNot(a => deps.contains(a.exprId)).foreach { a =>
          deps.update(a.exprId, other.references.toSeq.flatMap(depsOf).map(_.copy(transformation = Transformation.transformation)).toSet)
        }
        other match {
          case _: Project if children.size == 1 && isPassThrough(other) && label(other).isEmpty => children.headOption
          case v: View => Some(operation(v, children, "View", v.desc.identifier.unquotedString))
          case _       => Some(operation(other, children, other.nodeName, describe(other)))
        }
    }

    private def source(plan: LogicalPlan, ds: DatasetNode): Option[String] = {
      inputs.getOrElseUpdate(ds.id, ds)
      label(plan).foreach(l => dsLabels.getOrElseUpdate(ds.id, l))
      plan.output.foreach(a => deps.update(a.exprId, Set(InputDep(FieldRef(ds.namespace, ds.name, a.name), Transformation.identity))))
      Some(ds.id)
    }

    private def isInMemory(p: LogicalPlan): Boolean =
      p.collectLeaves().forall(l => l.isInstanceOf[LocalRelation] || l.isInstanceOf[LogicalRDD])

    private def isPassThrough(p: LogicalPlan): Boolean = p.expressions.forall(_.isInstanceOf[Attribute])

    private def label(p: LogicalPlan): Option[String] =
      labels.collectFirst { case (name, tagged) if tagged.sameResult(p) => name }

    private def operation(plan: LogicalPlan, children: Seq[String], kind: String, detail: String): String = {
      counter += 1
      val id = s"op$counter"
      operations += OperationNode(id, kind, label(plan), detail)
      children.foreach(c => edges += Edge(c, id))
      id
    }

    private def describe(p: LogicalPlan): String = {
      val text = p match {
        case j: Join      => s"${j.joinType.sql} ON ${j.condition.map(_.sql).getOrElse("true")}"
        case f: Filter    => f.condition.sql
        case a: Aggregate => s"GROUP BY ${a.groupingExpressions.map(_.sql).mkString(", ")}"
        case p: Project   => p.projectList.map(_.name).mkString(", ")
        case s: Sort      => s.order.map(_.sql).mkString(", ")
        case l: GlobalLimit => l.limitExpr.sql
        case _            => p.simpleString(80).stripPrefix(p.nodeName).trim
      }
      if (text.length > 120) text.take(117) + "..." else text
    }

    private def leafDataset(leaf: LeafNode): DatasetNode = {
      val schema = leaf.schema
      leaf match {
        case l: LogicalRelation =>
          l.catalogTable.map(tableNode(_, schema)).getOrElse {
            l.relation match {
              case fs: HadoopFsRelation =>
                val paths = fs.location.rootPaths.map(_.toUri)
                val ns    = paths.headOption.map(u => Option(u.getScheme).getOrElse("file")).getOrElse("file")
                DatasetNode(ns, paths.map(_.getPath).mkString(","), fields(schema))
              case r => DatasetNode("relation", r.getClass.getSimpleName, fields(schema))
            }
          }
        case r: DataSourceV2Relation =>
          DatasetNode(r.catalog.map(_.name).getOrElse("v2"), r.identifier.map(_.toString).getOrElse(r.table.name), fields(schema))
        case _: LocalRelation | _: LogicalRDD =>
          DatasetNode("memory", label(leaf).getOrElse(s"${leaf.nodeName}_${Integer.toHexString(leaf.semanticHash())}"), fields(schema))
        case _ =>
          DatasetNode("spark", label(leaf).getOrElse(leaf.nodeName), fields(schema))
      }
    }

  }
}
