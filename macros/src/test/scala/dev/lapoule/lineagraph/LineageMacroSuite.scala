package dev.lapoule.lineagraph

import dev.lapoule.lineagraph.Lineage._
import org.apache.spark.sql.functions._

class LineageMacroSuite extends SparkSuite {

  test("captures source position, enclosing val and code") {
    val revenue = lineage {
      csv("orders").groupBy("customer_id").agg(sum("amount").as("total"))
    }
    val src = revenue.graph.source
    assertEquals(src.file, "macros/src/test/scala/dev/lapoule/lineagraph/LineageMacroSuite.scala")
    assertEquals(src.line, 9)
    assertEquals(src.valName, Some("revenue"))
    assertEquals(revenue.graph.output.name, "revenue")
    assert(src.code.contains("""agg(sum("amount").as("total"))"""), src.code)
    assertEquals(deps(revenue.graph, "total"), Set(("orders", "amount", "AGGREGATION")))
  }

  test("vals inside the block and outer DataFrames become labels") {
    val customers = csv("customers")
    val traced = lineage {
      val paid = csv("orders").filter(col("status") === "PAID")
      paid.join(customers, "customer_id")
    }
    assertEquals(traced.graph.labels.values.toSet, Set("customers"))
    assertEquals(traced.graph.operations.find(_.kind == "Filter").flatMap(_.label), Some("paid"))
    assertEquals(traced.df.count(), 3L)
  }

  test("SQL literals are captured, directly or through a local val") {
    csv("orders").createOrReplaceTempView("orders")
    val direct = lineage(spark.sql("SELECT order_id FROM orders"))
    assertEquals(direct.graph.source.sql, Seq("SELECT order_id FROM orders"))

    val viaVal = lineage {
      val q = "SELECT amount * 2 AS twice FROM orders"
      spark.sql(q)
    }
    assertEquals(viaVal.graph.source.sql, Seq("SELECT amount * 2 AS twice FROM orders"))
    assertEquals(deps(viaVal.graph, "twice"), Set(("orders", "amount", "TRANSFORMATION")))
  }

  test("lambdas and UDFs inside the block survive the expansion") {
    val shout = udf((s: String) => s.toUpperCase + "!")
    val traced = lineage {
      val names = csv("customers").select(shout(col("name")).as("loud"))
      names.filter(r => r.getString(0).nonEmpty).toDF()
    }
    assertEquals(deps(traced.graph, "loud"), Set(("customers", "name", "TRANSFORMATION")))
  }

  test("lineageWrites returns the block result and one graph per write") {
    val out = dataDir.resolve("macro-out").toString
    val (n, graphs) = lineageWrites(spark) {
      val df = csv("orders").select(col("order_id"), (col("amount") * 100).as("cents"))
      df.write.mode("overwrite").parquet(out)
      df.count()
    }
    assertEquals(n, 4L)
    assertEquals(graphs.size, 1)
    assertEquals(deps(graphs.head, "cents"), Set(("orders", "amount", "TRANSFORMATION")))
    assertEquals(graphs.head.source.line, 56)
  }
}
