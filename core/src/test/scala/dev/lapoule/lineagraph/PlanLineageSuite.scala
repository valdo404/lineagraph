package dev.lapoule.lineagraph

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class PlanLineageSuite extends SparkSuite {

  private def revenueApi() =
    csv("orders").filter(col("status") === "PAID")
      .join(csv("customers"), "customer_id")
      .groupBy(col("customer_id"), col("country"))
      .agg(sum("amount").as("total_amount"))

  private def revenueSql() = {
    csv("orders").createOrReplaceTempView("orders")
    csv("customers").createOrReplaceTempView("customers")
    spark.sql(
      """SELECT o.customer_id, c.country, SUM(o.amount) AS total_amount
        |FROM orders o JOIN customers c ON o.customer_id = c.customer_id
        |WHERE o.status = 'PAID'
        |GROUP BY o.customer_id, c.country""".stripMargin)
  }

  for ((name, df) <- Seq("DataFrame API" -> (() => revenueApi()), "SQL" -> (() => revenueSql())))
    test(s"join + aggregate lineage ($name)") {
      val g = PlanLineage.extract(df())
      assertEquals(g.inputs.map(ds => shortName(ds.name)).toSet, Set("orders", "customers"))
      assertEquals(deps(g, "customer_id"), Set(("orders", "customer_id", "IDENTITY")))
      assertEquals(deps(g, "country"), Set(("customers", "country", "IDENTITY")))
      assertEquals(deps(g, "total_amount"), Set(("orders", "amount", "AGGREGATION")))
      val indirect = datasetDeps(g)
      assert(indirect(("orders", "customer_id", "JOIN")), indirect)
      assert(indirect(("customers", "customer_id", "JOIN")), indirect)
      assert(indirect(("orders", "status", "FILTER")), indirect)
      assert(indirect(("customers", "country", "GROUP_BY")), indirect)
      assert(g.operations.map(_.kind).contains("Join"))
    }

  test("expressions are TRANSFORMATION, nested aggregates stay AGGREGATION") {
    val g = PlanLineage.extract(
      csv("orders").groupBy("customer_id").agg((sum(col("amount") * 2) + 1).as("score"))
        .select(upper(col("customer_id").cast("string")).as("cid"), col("score")))
    assertEquals(deps(g, "cid"), Set(("orders", "customer_id", "TRANSFORMATION")))
    assertEquals(deps(g, "score"), Set(("orders", "amount", "AGGREGATION")))
  }

  test("CTE, union and scalar subquery") {
    csv("orders").createOrReplaceTempView("orders")
    csv("customers").createOrReplaceTempView("customers")
    val g = PlanLineage.extract(spark.sql(
      """WITH fr AS (SELECT customer_id, name FROM customers WHERE country = 'FR')
        |SELECT name AS who, (SELECT MAX(amount) FROM orders) AS top FROM fr
        |UNION ALL
        |SELECT CAST(order_id AS STRING), amount FROM orders""".stripMargin))
    assertEquals(deps(g, "who"), Set(("customers", "name", "IDENTITY"), ("orders", "order_id", "TRANSFORMATION")))
    assertEquals(deps(g, "top"), Set(("orders", "amount", "AGGREGATION"), ("orders", "amount", "IDENTITY")))
    assert(datasetDeps(g)(("customers", "country", "FILTER")))
  }

  test("window functions") {
    val w = Window.partitionBy("customer_id").orderBy(col("amount").desc)
    val g = PlanLineage.extract(csv("orders").withColumn("rank", row_number().over(w)).withColumn("running", sum("amount").over(w)))
    assertEquals(deps(g, "running"), Set(("orders", "amount", "AGGREGATION")))
    assert(datasetDeps(g)(("orders", "customer_id", "WINDOW")))
  }

  test("self join keeps both sides") {
    val o = csv("orders")
    val g = PlanLineage.extract(o.as("a").join(o.as("b"), col("a.customer_id") === col("b.customer_id"))
      .select(col("a.order_id").as("left_id"), col("b.order_id").as("right_id")))
    assertEquals(deps(g, "left_id"), Set(("orders", "order_id", "IDENTITY")))
    assertEquals(deps(g, "right_id"), Set(("orders", "order_id", "IDENTITY")))
    assertEquals(g.operations.count(_.kind == "Join"), 1)
  }

  test("in-memory data is named after its temp view") {
    import spark.implicits._
    Seq((1, "x")).toDF("id", "v").createOrReplaceTempView("mem")
    val g = PlanLineage.extract(spark.sql("SELECT id + 1 AS next FROM mem"))
    assertEquals(g.inputs.map(_.id), Seq("memory:mem"))
    assertEquals(deps(g, "next"), Set(("mem", "id", "TRANSFORMATION")))
  }

  test("write commands: output is the written path") {
    val out = dataDir.resolve("out").toString
    val (_, graphs) = WriteCapture(spark, model.SourceInfo.unknown) {
      revenueApi().write.mode("overwrite").parquet(out)
    }
    assertEquals(graphs.size, 1)
    val g = graphs.head
    assertEquals(g.output.namespace, "file")
    assert(g.output.name.endsWith("/out"), g.output.name)
    assertEquals(deps(g, "total_amount"), Set(("orders", "amount", "AGGREGATION")))
  }
}
