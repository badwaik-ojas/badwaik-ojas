import org.apache.spark.sql.SparkSession

object SparkRDDBroadcast extends App {
  val sparkSession = SparkSession
    .builder
    .master("local")
    .appName("RDD Creation")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  val rangeRDD = sc.range(1, 100).collect()

  val broadcastVar = sc.broadcast(rangeRDD)

  /*
  Broadcast => Broadcast variables allow the programmer to keep a read-only
  variable cached on each machine rather than shipping a copy of it with tasks.

  They can be used, for example, to give every node a copy of a large input
  dataset in an efficient manner. Spark also attempts to distribute broadcast
  variables using efficient broadcast algorithms to reduce communication cost.

  Broadcast variables are created from a variable v by calling
  SparkContext.broadcast(v). The broadcast variable is a wrapper around v,
  and its value can be accessed by calling the value method.
  The code below shows this:
   */

  broadcastVar.value.foreach(println)
}
