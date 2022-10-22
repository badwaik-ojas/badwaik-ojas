import org.apache.spark.sql.SparkSession

object RDDCreation extends App {

  //Create Spark Session
  val sparkSession = SparkSession
    .builder
    .master("local")
    .appName("RDD Creation")
    .getOrCreate()

  val sc = sparkSession.sparkContext
  //Create RDD using

  //By providing the List of values and setting numPartition = 3
  /*
  Note:
   Normally, Spark tries to set the number of partitions automatically
   based on your cluster.
   However, you can also set it manually by passing it as a second parameter
   to parallelize
   */
  val linesRDD = sc
    .parallelize(Seq("This is the first line", "This is the second line", "This is the third line"), 3)
  linesRDD.foreach(println)

  //Read the number of partition
  println("*********** partitions: "+linesRDD.partitions.size)
  //Read the number of partition
  println("*********** getNumPartitions: "+linesRDD.getNumPartitions)

  //By providing File Path and setting numPartition = 3
  /*
  Notes:
  The textFile method also takes an optional second argument for
  controlling the number of partitions of the file.
  By default, Spark creates one partition for each block of the file
  (blocks being 128MB by default in HDFS), but you can also ask for a
  higher number of partitions by passing a larger value.
   */
  val readFileRDD = sc.textFile("src/main/resources/readTextRDDExample.txt", 3)
  readFileRDD.foreach(println)

}
