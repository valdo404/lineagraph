package dev.lapoule.lineagraph.exporters

import dev.lapoule.lineagraph.model._
import io.openlineage.client.{OpenLineage, OpenLineageClientUtils}

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.time.ZonedDateTime
import java.util.UUID
import scala.jdk.CollectionConverters._

/** Renders a [[LineageGraph]] as an OpenLineage `RunEvent` (COMPLETE). */
object OpenLineageExporter {

  val Producer: URI = URI.create("https://github.com/lapoule/lineagraph")

  def toEvent(graph: LineageGraph, jobNamespace: String = "lineagraph", runId: UUID = UUID.randomUUID()): OpenLineage.RunEvent = {
    val ol = new OpenLineage(Producer)

    def schemaFacet(ds: DatasetNode) =
      ol.newSchemaDatasetFacetBuilder()
        .fields(ds.fields.map(f => ol.newSchemaDatasetFacetFieldsBuilder().name(f.name).`type`(f.dataType).build()).asJava)
        .build()

    def inputField(dep: InputDep) =
      ol.newInputFieldBuilder()
        .namespace(dep.field.namespace)
        .name(dep.field.name)
        .field(dep.field.field)
        .transformations(List(
          ol.newInputFieldTransformationsBuilder()
            .`type`(dep.transformation.kind.value)
            .subtype(dep.transformation.subtype.value)
            .masking(false)
            .build()
        ).asJava)
        .build()

    val columnFields = graph.columns.foldLeft(ol.newColumnLineageDatasetFacetFieldsBuilder()) { (b, col) =>
      b.put(col.output, ol.newColumnLineageDatasetFacetFieldsAdditionalBuilder().inputFields(col.inputs.map(inputField).asJava).build())
    }.build()

    val columnLineage = ol.newColumnLineageDatasetFacetBuilder()
      .fields(columnFields)
      .dataset(graph.datasetDeps.map(inputField).asJava)
      .build()

    val inputs = graph.inputs.map { ds =>
      ol.newInputDatasetBuilder()
        .namespace(ds.namespace)
        .name(ds.name)
        .facets(ol.newDatasetFacetsBuilder().schema(schemaFacet(ds)).build())
        .build()
    }

    val output = ol.newOutputDatasetBuilder()
      .namespace(graph.output.namespace)
      .name(graph.output.name)
      .facets(ol.newDatasetFacetsBuilder().schema(schemaFacet(graph.output)).columnLineage(columnLineage).build())
      .build()

    val src = graph.source
    val jobFacets = ol.newJobFacetsBuilder()
      .jobType(ol.newJobTypeJobFacetBuilder().processingType("BATCH").integration("SPARK").jobType("QUERY").build())
      .sourceCode(ol.newSourceCodeJobFacet("scala", src.code))
      .sourceCodeLocation(ol.newSourceCodeLocationJobFacetBuilder()
        .`type`("file")
        .url(new java.io.File(src.file).toURI)
        .path(s"${src.file}:${src.line}")
        .build())
    if (src.sql.nonEmpty) jobFacets.sql(ol.newSQLJobFacetBuilder().query(src.sql.mkString(";\n")).dialect("spark").build())

    ol.newRunEventBuilder()
      .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
      .eventTime(ZonedDateTime.now())
      .run(ol.newRunBuilder().runId(runId).build())
      .job(ol.newJobBuilder().namespace(jobNamespace).name(src.jobName).facets(jobFacets.build()).build())
      .inputs(inputs.asJava)
      .outputs(List(output).asJava)
      .build()
  }

  def toJson(graph: LineageGraph): String = OpenLineageClientUtils.toJson(toEvent(graph))

  def write(graph: LineageGraph, dir: Path): Path = {
    Files.createDirectories(dir)
    val file = dir.resolve(s"${graph.source.jobName.replaceAll("[^A-Za-z0-9._-]", "_")}.json")
    Files.write(file, toJson(graph).getBytes(StandardCharsets.UTF_8))
  }
}
