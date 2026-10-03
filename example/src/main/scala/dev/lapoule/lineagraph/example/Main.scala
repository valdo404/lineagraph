package dev.lapoule.lineagraph.example

import dev.lapoule.lineagraph.exporters.{MermaidExporter, OpenLineageExporter}
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Paths}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("lineagraph-example")
      .config("spark.ui.enabled", "false").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val outDir = Paths.get("target/lineage")
    val job    = new JoinJob(spark, "example/src/main/resources/data")

    try {
      for ((name, traced) <- Seq("dataframe-api" -> job.dataFrameApi(), "sql" -> job.sql())) {
        println(s"\n===== $name =====")
        traced.df.show(truncate = false)
        val mermaid = MermaidExporter.render(traced.graph)
        println(mermaid)
        Files.createDirectories(outDir)
        Files.writeString(outDir.resolve(s"$name.mmd"), mermaid)
        println(s"OpenLineage event -> ${OpenLineageExporter.write(traced.graph, outDir.resolve(name))}")

        job.write(traced.df, s"target/out/$name").foreach { g =>
          println(s"write lineage -> ${OpenLineageExporter.write(g, outDir.resolve(s"$name-write"))}")
        }
      }
    } finally spark.stop()
  }
}
