import SparkDataframe.spark
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}



object SparkDatasets extends App {

  val spark = SparkSession
    .builder
    .master("local")
    .appName("SparkDatasets")
    .getOrCreate()

  case class Person(name: String, age: Long)

  import spark.implicits._
  // Encoders are created for case classes
  val caseClassDS = Seq(Person("Ojas",32)).toDS()
  caseClassDS.show()

  // Encoders for most common types are automatically provided by importing spark.implicits._
  val primitiveDS = Seq(1, 2, 3).toDS()
  primitiveDS.show()
  primitiveDS.printSchema()

  // DataFrames can be converted to a Dataset by providing a class.
  // Mapping will be done by name

  val createDS = spark.read.json("src/main/resources/people.json").as[Person]
  createDS.show()

  //Inferring schema

  val peopleDF = spark.sparkContext
    .textFile("src/main/resources/people.txt")
    .map(_.split(","))
    .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
    .toDF()

  // Register the DataFrame as a temporary view
  peopleDF.createOrReplaceTempView("people")

  // SQL statements can be run by using the sql methods provided by Spark
  val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")

  // The columns of a row in the result can be accessed by field index
  teenagersDF.map(teenager => "Name: " + teenager(0)).show()

  // or by field name
  teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()

  //Programmatically Specifying the Schema

  // Create an RDD
  val peopleRDD = spark.sparkContext.textFile("examples/src/main/resources/people.txt")

  // The schema is encoded in a string
  val schemaString = "name age"

  // Generate the schema based on the string of schema
  val fields = schemaString.split(" ")
    .map(fieldName => StructField(fieldName, StringType, nullable = true))
  val schema = StructType(fields)

  // Convert records of the RDD (people) to Rows
  val rowRDD = peopleRDD
    .map(_.split(","))
    .map(attributes => Row(attributes(0), attributes(1).trim))

  // Apply the schema to the RDD
  val peopleDFNew = spark.createDataFrame(rowRDD, schema)

  // Creates a temporary view using the DataFrame
  peopleDFNew.createOrReplaceTempView("people")

  // SQL can be run over a temporary view created using DataFrames
  val results = spark.sql("SELECT name FROM people")

  // The results of SQL queries are DataFrames and support all the normal RDD operations
  // The columns of a row in the result can be accessed by field index or by field name
  results.map(attributes => "Name: " + attributes(0)).show()


}
