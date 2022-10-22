import org.apache.spark.sql.SparkSession

object SparkRDDTransformations extends App {

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Spark Transformation")
    .getOrCreate()

  val sc = spark.sparkContext

  val linesRDD = sc.textFile("src/main/resources/readTextRDDExample.txt", 1)

  /*
  Flatmap => this transformation flattens the RDD after applying
  the function and returns a new RDD. At first it splits the records by space in
  an RDD and finally it flattens it. Resulting record are single
  word on each record.

  Input RDD =>
  {
    This is the first line
    This is the second line
    This is the third line
  }

  Output RDD =>
  {
  This
  is
  the
  first
  line
  This
  is
  the
  second
  line
  This
  is
  the
  third
  line
  }
   */
  val flattenRDD = linesRDD.flatMap(line => line.split(" "))
  flattenRDD.foreach(println)

  /*
  Map() => This transformation is used to apply any complex operation like
  adding a column, updating a column.
  The output of the transformation will have the same number of rows as the original

  Let us add a new column to the above flattened RDD and the new column will have 1
  constant value.

  Input RDD =>
    {
    This
    is
    the
    first
    line
    This
    is
    the
    second
    line
    This
    is
    the
    third
    line
    }

  Output RDD =>
  {
  (This,1)
  (is,1)
  (the,1)
  (first,1)
  (line,1)
  (This,1)
  (is,1)
  (the,1)
  (second,1)
  (line,1)
  (This,1)
  (is,1)
  (the,1)
  (third,1)
  (line,1)
  }
   */

  val mapRDD = flattenRDD.map(wordMap => (wordMap,1))
  mapRDD.foreach(println)

  /*
  Filter() => This function is used to filter the records in RDD.
  Below example we will be filtering the records from RDD which has record that start with "T"

  Input RDD =>
    {
    This
    is
    the
    first
    line
    This
    is
    the
    second
    line
    This
    is
    the
    third
    line
    }

   Output RDD =>
   {
    This
    This
    This
    }
   */

  val filterRDD = flattenRDD.filter(filter => filter.startsWith("T"))
  filterRDD.foreach(println)

  val rangeRDD = sc.range(1, 100, numSlices=2)
  rangeRDD.foreach(println)

  val evenOddRDD = rangeRDD.map( m => if(m%2==0) ("even", m) else ("odd", m) )

  /*
  groupByKey => This groups the RDD into Key value pair

   */
  sc.setLocalProperty("callSite.short","groupByKey Operation")
  val groupByKeyRDD = evenOddRDD.groupByKey().foreach(println)

  /*
  reduceByKey => When called on the dataset, returns a dataset of (K, V)
  pairs where the values for each key are aggregated using the given
  reduce function func, which must be of type (V,V) => V

   The number of reduce tasks is configurable through an optional second argument.
   */

  sc.setLocalProperty("callSite.short", "reduceByKey Operation")
  val reduceByKeyRDD = evenOddRDD.reduceByKey((x,y) => x+y).foreach(println)

  /*
    aggregateByKey => When called on a dataset of (K, V) pairs, returns a
    dataset of (K, U) pairs where the values for each key are aggregated using
     the given combine functions and a neutral "zero" value.

     Allows an aggregated value type that is different than the input value
     type, while avoiding unnecessary allocations.
  */
  sc.setLocalProperty("callSite.short", "aggregateByKey Operation")
  //val aggregateByKeyRDD = evenOddRDD.aggregateByKey(0)(_+_,_+_).foreach(println)

  Thread.sleep(3000000)

}
