import org.apache.spark.sql.SparkSession

object SparkDataframeWithColumn extends App {

  val spark = SparkSession
    .builder
    .master("local")
    .appName("SparkDataframeWithColumn")
    .getOrCreate()



}
