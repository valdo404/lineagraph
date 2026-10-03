package dev.lapoule.lineagraph.model

/** Where the traced code lives. Filled at compile time by the `lineage` macro. */
final case class SourceInfo(
    file: String,
    line: Int,
    owner: String,
    valName: Option[String],
    code: String,
    sql: Seq[String] = Nil
) {
  def jobName: String = valName.fold(owner)(v => s"$owner.$v")
}

object SourceInfo {
  val unknown: SourceInfo = SourceInfo("<unknown>", 0, "<unknown>", None, "")
}

final case class Field(name: String, dataType: String)

final case class DatasetNode(namespace: String, name: String, fields: Seq[Field]) {
  def id: String = s"$namespace:$name"
}

final case class FieldRef(namespace: String, name: String, field: String) {
  def datasetId: String = s"$namespace:$name"
  override def toString: String = s"$name.$field"
}

/** Mirrors the OpenLineage `columnLineage` facet transformation vocabulary. */
sealed abstract class TransformationType(val value: String)
object TransformationType {
  case object Direct   extends TransformationType("DIRECT")
  case object Indirect extends TransformationType("INDIRECT")
}

sealed abstract class TransformationSubtype(val value: String, val rank: Int)
object TransformationSubtype {
  // DIRECT subtypes, ordered: combining two keeps the strongest one.
  case object Identity       extends TransformationSubtype("IDENTITY", 0)
  case object Transformation extends TransformationSubtype("TRANSFORMATION", 1)
  case object Aggregation    extends TransformationSubtype("AGGREGATION", 2)
  // INDIRECT subtypes.
  case object Join        extends TransformationSubtype("JOIN", 0)
  case object Filter      extends TransformationSubtype("FILTER", 0)
  case object GroupBy     extends TransformationSubtype("GROUP_BY", 0)
  case object Sort        extends TransformationSubtype("SORT", 0)
  case object Window      extends TransformationSubtype("WINDOW", 0)
  case object Conditional extends TransformationSubtype("CONDITIONAL", 0)
}

final case class Transformation(kind: TransformationType, subtype: TransformationSubtype) {
  override def toString: String = s"${kind.value}/${subtype.value}"
}

object Transformation {
  val identity: Transformation       = Transformation(TransformationType.Direct, TransformationSubtype.Identity)
  val transformation: Transformation = Transformation(TransformationType.Direct, TransformationSubtype.Transformation)
  val aggregation: Transformation    = Transformation(TransformationType.Direct, TransformationSubtype.Aggregation)
  def indirect(subtype: TransformationSubtype): Transformation = Transformation(TransformationType.Indirect, subtype)

  /** Applying `outer` on top of `inner` (both DIRECT) keeps the strongest effect. */
  def compose(inner: Transformation, outer: Transformation): Transformation =
    if (outer.subtype.rank >= inner.subtype.rank) outer else inner
}

final case class InputDep(field: FieldRef, transformation: Transformation)

final case class ColumnLineage(output: String, inputs: Seq[InputDep])

/** One step of the logical plan, kept for the dataset-level graph. */
final case class OperationNode(id: String, kind: String, label: Option[String], detail: String)

final case class Edge(from: String, to: String)

final case class LineageGraph(
    source: SourceInfo,
    inputs: Seq[DatasetNode],
    output: DatasetNode,
    operations: Seq[OperationNode],
    edges: Seq[Edge],
    columns: Seq[ColumnLineage],
    /** Fields that influence the whole output (join keys, filters, group by...). */
    datasetDeps: Seq[InputDep],
    /** Dataset id -> name of the Scala val holding it, when the macro could see one. */
    labels: Map[String, String] = Map.empty
) {
  def column(name: String): Option[ColumnLineage] = columns.find(_.output == name)
}
