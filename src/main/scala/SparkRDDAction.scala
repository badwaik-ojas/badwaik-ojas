import SparkRDDTransformations.sc
import org.apache.spark.sql.SparkSession

object SparkRDDAction extends App{

  //Create Spark Session
  val sparkSession = SparkSession
    .builder
    .master("local")
    .appName("RDD Creation")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  val rangeRDD = sc.range(1, 100)

  /*
  Reduce => Aggregate the elements of the dataset using a function
  func (which takes two arguments and returns one).

  The function should be commutative and associative so that it can be
  computed correctly in parallel.
   */
  sc.setLocalProperty("callSite.short", "reduce Operation")
  val reduceRDD = rangeRDD.reduce(_+_)
  println("reduceRDD: "+reduceRDD)

  /*
  Collect => Return all the element of the dataset as an array at the driver program.
  This usually useful after the filter operation. that returns a small set of data
   */

  println(rangeRDD.collect())

  /*
  Count => Return the number of elements in the dataset.
   */

  println(rangeRDD.count())

  /*
  Other Actions:
  first() => Return the first element of the dataset (similar to take(1)).
  take(n) => Return an array with the first n elements of the dataset.
  countByKey() => Only available on RDDs of type (K, V).
  saveAsTextFile(path) => Write the elements of the dataset as a text file (
    or set of text files) in a given directory in the local filesystem,
    HDFS or any other Hadoop-supported file system.
    Spark will call toString on each element to convert it to a line of text
    in the file.
   */
  Thread.sleep(3000000)

}
