package SparkInAction

import org.apache.spark.sql.SparkSession

object Test extends App {

  val spark1 = SparkSession
    .builder()
    .appName("Spark Session Demo1")
    .master("local")
    .getOrCreate()

  println("Spark Session Created")

  import spark1.implicits._
  val rdd = spark1.sparkContext.textFile("src/main/resources/people.txt")
  rdd.foreach(println)


  val df= rdd.toDF()

}
