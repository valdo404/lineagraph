package dev.lapoule.lineagraph

import dev.lapoule.lineagraph.exporters.{MermaidExporter, OpenLineageExporter}
import dev.lapoule.lineagraph.model.SourceInfo
import io.openlineage.client.OpenLineageClientUtils
import org.apache.spark.sql.functions._

import scala.jdk.CollectionConverters._

class ExportersSuite extends SparkSuite {

  private lazy val graph = PlanLineage.extract(
    csv("orders").join(csv("customers"), "customer_id").groupBy("country").agg(sum("amount").as("total")),
    SourceInfo("Job.scala", 42, "dev.lapoule.Job.run", Some("revenue"), "orders.join(customers)", Seq("SELECT 1"))
  )

  test("OpenLineage RunEvent round-trips through the official client") {
    val event = OpenLineageClientUtils.runEventFromJson(OpenLineageExporter.toJson(graph))
    assertEquals(event.getEventType.toString, "COMPLETE")
    assertEquals(event.getJob.getName, "dev.lapoule.Job.run.revenue")
    assertEquals(event.getJob.getFacets.getSql.getQuery, "SELECT 1")
    assertEquals(event.getInputs.size, 2)

    val output = event.getOutputs.get(0)
    assertEquals(output.getName, "revenue")
    val total = output.getFacets.getColumnLineage.getFields.getAdditionalProperties.get("total")
    val in    = total.getInputFields.asScala.map(f => (f.getField, f.getTransformations.get(0).getSubtype)).toSet
    assertEquals(in, Set("amount" -> "AGGREGATION"))
    val indirect = output.getFacets.getColumnLineage.getDataset.asScala.map(f => f.getTransformations.get(0).getSubtype).toSet
    assertEquals(indirect, Set("JOIN", "GROUP_BY"))
  }

  test("Mermaid flowchart") {
    val mmd = MermaidExporter.render(graph)
    assert(mmd.startsWith("flowchart LR"))
    assert(mmd.contains("orders.csv.amount"))
    assert(mmd.contains("-->|AGGREGATION|"))
    assert(mmd.contains("-.->|"))
  }
}
