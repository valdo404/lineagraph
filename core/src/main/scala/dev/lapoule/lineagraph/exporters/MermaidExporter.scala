package dev.lapoule.lineagraph.exporters

import dev.lapoule.lineagraph.model._

/** Renders a [[LineageGraph]] as a Mermaid flowchart (datasets, operations, then column lineage). */
object MermaidExporter {

  def render(graph: LineageGraph, columns: Boolean = true): String = {
    val sb  = new StringBuilder("flowchart LR\n")
    val ids = (graph.inputs.map(_.id) :+ graph.output.id).zipWithIndex.map { case (id, i) => id -> s"ds$i" }.toMap

    def node(id: String): String = ids.getOrElse(id, id)
    def esc(s: String): String   = s.replace("\"", "#quot;").replace("<", "#lt;").replace(">", "#gt;")
    // Paths are long: show the last segment, the full name lives in the OpenLineage event.
    def short(name: String): String = name.split(",").map(_.split('/').filter(_.nonEmpty).lastOption.getOrElse(name)).mkString(", ")
    def shortField(f: FieldRef): String = s"${short(f.name)}.${f.field}"

    sb ++= "  subgraph tables [Datasets & operations]\n"
    graph.inputs.foreach { ds =>
      val title = graph.labels.get(ds.id).fold(esc(short(ds.name)))(l => s"<b>${esc(l)}</b><br/>${esc(short(ds.name))}")
      sb ++= s"""    ${node(ds.id)}[("$title")]\n"""
    }
    graph.operations.foreach { op =>
      val title = op.label.fold(op.kind)(l => s"${op.kind} · $l")
      sb ++= s"""    ${op.id}["<b>${esc(title)}</b><br/>${esc(op.detail)}"]\n"""
    }
    sb ++= s"""    ${node(graph.output.id)}[["${esc(short(graph.output.name))}"]]\n"""
    graph.edges.foreach(e => sb ++= s"    ${node(e.from)} --> ${node(e.to)}\n")
    sb ++= "  end\n"

    if (columns) {
      val inputFields = (graph.columns.flatMap(_.inputs) ++ graph.datasetDeps).map(_.field).distinct
      val fieldIds    = inputFields.zipWithIndex.map { case (f, i) => f -> s"in$i" }.toMap
      sb ++= "  subgraph cols [Column lineage]\n"
      inputFields.foreach(f => sb ++= s"""    ${fieldIds(f)}("${esc(shortField(f))}")\n""")
      graph.columns.zipWithIndex.foreach { case (c, i) =>
        sb ++= s"""    out$i["${esc(s"${short(graph.output.name)}.${c.output}")}"]\n"""
        c.inputs.foreach(d => sb ++= s"    ${fieldIds(d.field)} -->|${d.transformation.subtype.value}| out$i\n")
      }
      graph.datasetDeps.groupBy(_.field).foreach { case (f, ds) =>
        sb ++= s"    ${fieldIds(f)} -.->|${ds.map(_.transformation.subtype.value).distinct.mkString(",")}| ${node(graph.output.id)}\n"
      }
      sb ++= "  end\n"
    }
    sb.toString
  }
}
