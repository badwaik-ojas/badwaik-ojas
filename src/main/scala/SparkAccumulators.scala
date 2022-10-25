import org.apache.spark.sql.SparkSession

object SparkAccumulators extends App {

  val sparkSession = SparkSession
    .builder
    .master("local")
    .appName("SparkAccumulators")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  val rangeRDD = sc.range(1, 100)

  /*

  Accumulators =>
  Accumulators are variables that are only “added” to through an associative
  and commutative operation and can therefore be efficiently supported
  in parallel. They can be used to implement counters (as in MapReduce)
  or sums. Spark natively supports accumulators of numeric types, and
  programmers can add support for new types.

  As a user, you can create named or unnamed accumulators. As seen in the
  image below, a named accumulator (in this instance counter) will display
  in the web UI for the stage that modifies that accumulator. Spark displays
  the value for each accumulator modified by a task in the “Tasks” table.

  A numeric accumulator can be created by calling SparkContext.longAccumulator()
  or SparkContext.doubleAccumulator() to accumulate values of type Long or
  Double, respectively. Tasks running on a cluster can then add to it using the
  add method. However, they cannot read its value. Only the driver program can
  read the accumulator’s value, using its value method.

   */

  val acc = sc.longAccumulator("acc")

  rangeRDD.foreach(x => acc.add(x))

  println("Total: "+acc.value)
}
