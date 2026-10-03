package dev.lapoule.lineagraph.example

import dev.lapoule.lineagraph.Lineage._
import dev.lapoule.lineagraph.model.LineageGraph
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Revenue per customer: orders ⋈ customers, paid orders only. Same job written twice. */
final class JoinJob(spark: SparkSession, dataDir: String) {

  private def csv(name: String): DataFrame =
    spark.read.option("header", "true").option("inferSchema", "true").csv(s"$dataDir/$name.csv")

  def dataFrameApi(): Traced = lineage {
    val orders    = csv("orders").filter(col("status") === "PAID")
    val customers = csv("customers")
    orders
      .join(customers, "customer_id")
      .groupBy(col("customer_id"), col("name"), col("country"))
      .agg(sum("amount").as("total_amount"), count("order_id").as("order_count"))
      .withColumn("label", concat_ws(" - ", col("name"), col("country")))
  }

  def sql(): Traced = {
    csv("orders").createOrReplaceTempView("orders")
    csv("customers").createOrReplaceTempView("customers")
    val revenue = lineage {
      spark.sql(
        """SELECT o.customer_id, c.name, c.country,
          |       SUM(o.amount) AS total_amount, COUNT(o.order_id) AS order_count,
          |       concat_ws(' - ', c.name, c.country) AS label
          |FROM orders o JOIN customers c ON o.customer_id = c.customer_id
          |WHERE o.status = 'PAID'
          |GROUP BY o.customer_id, c.name, c.country""".stripMargin)
    }
    revenue
  }

  def write(df: DataFrame, outDir: String): Seq[LineageGraph] =
    lineageWrites(spark) {
      df.write.mode("overwrite").parquet(outDir)
    }._2
}
