package org.apache.spark.lineagraph

import org.apache.spark.SparkContext

/** Bridge to `private[spark]` APIs: QueryExecutionListener callbacks are delivered asynchronously. */
object ListenerBusAccess {
  def drain(sc: SparkContext): Unit = sc.listenerBus.waitUntilEmpty()
}
