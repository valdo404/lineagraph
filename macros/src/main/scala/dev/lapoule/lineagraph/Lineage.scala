package dev.lapoule.lineagraph

import dev.lapoule.lineagraph.model.LineageGraph
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.language.experimental.macros

/**
 * Entry points. Usage:
 * {{{
 *   import dev.lapoule.lineagraph.Lineage._
 *
 *   val report = lineage {
 *     val orders    = spark.read.option("header", "true").csv("orders.csv")
 *     val customers = spark.table("customers")
 *     orders.join(customers, "customer_id").groupBy("country").agg(sum("amount"))
 *   }
 *   report.graph   // dataset + column lineage
 *   report.df      // the DataFrame itself, untouched
 * }}}
 */
object Lineage {

  final case class Traced(df: DataFrame, graph: LineageGraph)

  /** Lineage of the DataFrame produced by `df`; vals defined in the block become graph labels. */
  def lineage(df: DataFrame): Traced = macro LineageMacros.trace

  /** Runs `block` and returns the lineage of every write it performed. */
  def lineageWrites[A](spark: SparkSession)(block: A): (A, Seq[LineageGraph]) = macro LineageMacros.traceWrites[A]
}
