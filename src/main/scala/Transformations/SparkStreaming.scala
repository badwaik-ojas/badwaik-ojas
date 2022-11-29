package Transformations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.ProcessingTimeTrigger
import org.apache.spark.sql.streaming.Trigger.ProcessingTime

object SparkStreaming extends App {

  /*
  Install NetCat:
  Open Command Prompt: execute command
  nc -l -p 9999
  Type any message
   */


  val spark = SparkSession
    .builder
    .master("local")
    .appName("Structured Streaming  WordCount")
    .getOrCreate()

  import spark.implicits._

  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()

  // Split the lines into words
  val words = lines.as[String].flatMap(_.split(" "))
  //words.show()

  // Generate running word count
  val wordCounts = words.groupBy("value").count()

  val query = wordCounts.writeStream
    .outputMode("complete")
    .trigger(ProcessingTime("10 seconds"))
    .format("console")
    .start()

  //Thread.sleep(10000)

  query.awaitTermination()

}
