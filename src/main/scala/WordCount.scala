import org.apache.spark.sql.SparkSession

object WordCount extends App{

  val spark = SparkSession.builder
    .master("local")
    .appName("Spark Word Count")
    .getOrCreate()

  import spark.implicits._
  val linesRDD = spark.sparkContext.parallelize(Seq("This is the first line", "This is the second line", "This is the third line"))

  //Covert to DF to see the values
  val linesDF = linesRDD.toDF("Lines")
  linesDF.show()

  //Convert to wordsDF
  val wordsRDD = linesRDD.flatMap(line => line.split(" ").map(word => (word,1)))
  val wordsDF = wordsRDD.toDF("words","count")
  wordsDF.show()

  val count = wordsRDD.reduceByKey(_+_)
  count.foreach(println)

}
