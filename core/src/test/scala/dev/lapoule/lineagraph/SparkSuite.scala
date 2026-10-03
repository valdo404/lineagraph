package dev.lapoule.lineagraph

import dev.lapoule.lineagraph.model._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.{Files, Path}

trait SparkSuite extends munit.FunSuite {

  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[2]")
    .appName(getClass.getSimpleName)
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  lazy val dataDir: Path = {
    val dir = Files.createTempDirectory("lineagraph")
    Files.writeString(dir.resolve("customers.csv"),
      "customer_id,name,country\n1,Alice,FR\n2,Bob,DE\n3,Chloe,FR\n")
    Files.writeString(dir.resolve("orders.csv"),
      "order_id,customer_id,amount,status\n10,1,12.5,PAID\n11,1,3.0,PAID\n12,2,99.0,CANCELLED\n13,3,7.0,PAID\n")
    dir
  }

  def csv(name: String): DataFrame =
    spark.read.option("header", "true").option("inferSchema", "true").csv(dataDir.resolve(s"$name.csv").toString)

  def deps(g: LineageGraph, column: String): Set[(String, String, String)] =
    g.column(column).getOrElse(fail(s"no column $column in ${g.columns.map(_.output)}"))
      .inputs.map(d => (shortName(d.field.name), d.field.field, d.transformation.subtype.value)).toSet

  def datasetDeps(g: LineageGraph): Set[(String, String, String)] =
    g.datasetDeps.map(d => (shortName(d.field.name), d.field.field, d.transformation.subtype.value)).toSet

  def shortName(name: String): String = name.split('/').last.stripSuffix(".csv")
}
