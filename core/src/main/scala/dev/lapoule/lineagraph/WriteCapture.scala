package dev.lapoule.lineagraph

import dev.lapoule.lineagraph.model.{LineageGraph, SourceInfo}
import org.apache.spark.lineagraph.ListenerBusAccess
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters._

/** Records the lineage of every write (save, saveAsTable, insertInto...) executed by `block`. */
object WriteCapture {

  def apply[A](spark: SparkSession, source: SourceInfo)(block: => A): (A, Seq[LineageGraph]) = {
    val captured = new ConcurrentLinkedQueue[LineageGraph]()
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
        PlanLineage.extractWrite(qe.analyzed, source).foreach(captured.add)
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = ()
    }
    spark.listenerManager.register(listener)
    try {
      val result = block
      ListenerBusAccess.drain(spark.sparkContext)
      (result, captured.asScala.toSeq)
    } finally spark.listenerManager.unregister(listener)
  }
}
