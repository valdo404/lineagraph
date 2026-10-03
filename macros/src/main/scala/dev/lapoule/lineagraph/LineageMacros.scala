package dev.lapoule.lineagraph

import scala.reflect.macros.blackbox

/**
 * Compile-time half of the hybrid approach. The macro knows nothing about schemas: it only
 * captures what the Spark plan forgets — where the code lives, how it is written, the names
 * of the intermediate DataFrames and the SQL literals — and then delegates to [[PlanLineage]].
 */
class LineageMacros(val c: blackbox.Context) {
  import c.universe._

  private val Pkg        = q"_root_.dev.lapoule.lineagraph"
  private val DatasetTpe = typeOf[org.apache.spark.sql.Dataset[_]]
  private val SessionTpe = typeOf[org.apache.spark.sql.SparkSession]
  private val SqlMethods = Set("sql")

  def trace(df: Tree): Tree = {
    val labels = TermName(c.freshName("labels"))
    val result = TermName(c.freshName("df"))

    // Stable DataFrame vals from the enclosing scope used by the block: label them up front.
    val definedInside = df.collect { case v: ValDef => v.symbol }.toSet
    val outer = df.collect {
      case i: Ident if isDataFrame(i.tpe) && i.symbol.isTerm && i.symbol.asTerm.isStable && !definedInside(i.symbol) => i.symbol
    }.distinct
    val outerLabels = outer.map(s => register(labels, s.name.decodedName.toString.trim, Ident(s.name)))

    // Inside the block: label each DataFrame val right after its definition.
    val body = c.untypecheck(df) match {
      case Block(stats, expr) =>
        val typedVals = df match {
          case Block(ts, _) => ts.collect { case v: ValDef if isDataFrame(v.rhs.tpe) => v.name }.toSet
          case _            => Set.empty[TermName]
        }
        val labelled = stats.flatMap {
          case v: ValDef if typedVals(v.name) => List(v, register(labels, v.name.decodedName.toString.trim, Ident(v.name)))
          case other                          => List(other)
        }
        Block(labelled, expr)
      case other => other
    }

    q"""{
      val $labels = _root_.scala.collection.mutable.LinkedHashMap.empty[String, _root_.org.apache.spark.sql.catalyst.plans.logical.LogicalPlan]
      ..$outerLabels
      val $result: _root_.org.apache.spark.sql.DataFrame = $body
      $Pkg.Lineage.Traced($result, $Pkg.PlanLineage.extract($result, ${sourceInfo(df)}, $labels.toMap))
    }"""
  }

  def traceWrites[A: c.WeakTypeTag](spark: Tree)(block: Tree): Tree =
    q"$Pkg.WriteCapture(${c.untypecheck(spark)}, ${sourceInfo(block)})(${c.untypecheck(block)})"

  private def register(labels: TermName, name: String, df: Tree): Tree =
    q"$labels.update($name, $df.queryExecution.analyzed)"

  private def isDataFrame(t: Type): Boolean = t != null && t <:< DatasetTpe

  private def sourceInfo(tree: Tree): Tree = {
    val pos   = c.enclosingPosition
    val root  = sys.props.getOrElse("user.dir", "")
    val path  = pos.source.path
    val file  = if (root.nonEmpty && path.startsWith(root + "/")) path.drop(root.length + 1) else path
    val code  =
      if (tree.pos.isRange) new String(pos.source.content.slice(tree.pos.start, tree.pos.end))
      else showCode(tree)
    val owner   = c.internal.enclosingOwner
    val valName = if (owner.isTerm && !owner.isMethod && !owner.isSynthetic) Some(owner.name.decodedName.toString.trim) else None
    q"$Pkg.model.SourceInfo($file, ${pos.line}, ${ownerName(owner)}, $valName, $code, ${sqlLiterals(tree)})"
  }

  /** `Main.run` style name: enclosing class/object and method, skipping vals and anonymous owners. */
  private def ownerName(owner: Symbol): String = {
    val chain = Iterator.iterate(owner)(_.owner).takeWhile(s => s != NoSymbol && !s.isPackage).toList
    val method = chain.find(s => s.isMethod && !s.isSynthetic && !s.name.decodedName.toString.contains("$anon"))
    val cls    = chain.find(s => s.isClass && !s.isSynthetic && !s.name.decodedName.toString.contains("$anon"))
    (cls.map(_.fullName).toList ++ method.map(_.name.decodedName.toString)).mkString(".")
  }

  /** String literals passed to `spark.sql(...)`, directly or through a val defined in the block. */
  private def sqlLiterals(tree: Tree): List[String] = {
    val constants = tree.collect { case v @ ValDef(_, _, _, Literal(Constant(s: String))) => v.symbol -> s }.toMap
    tree.collect {
      case Apply(Select(qual, name), arg :: _) if SqlMethods(name.decodedName.toString) && qual.tpe != null && qual.tpe <:< SessionTpe =>
        arg match {
          case Literal(Constant(s: String))       => Some(s)
          case i: Ident if constants.contains(i.symbol) => constants.get(i.symbol)
          case _                                  => None
        }
    }.flatten.map(_.trim)
  }
}
