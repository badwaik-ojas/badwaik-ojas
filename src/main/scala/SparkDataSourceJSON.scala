import org.apache.spark.sql.SparkSession

object SparkDataSourceJSON extends App {

  val spark = SparkSession
    .builder
    .master("local")
    .appName("SparkDataSourceJSON")
    .getOrCreate()

  val peopleDF = spark.read.json("src/main/resources/people.json")
  peopleDF.show()


}
