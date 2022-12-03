package SparkInAction

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructField, StructType}

object SparkSchema extends App {

  /*
  Spark Schema defines the structure of the DataFrame which you can get by calling
  printSchema() method on the DataFrame object. Spark SQL provides StructType & StructField
  classes to programmatically specify the schema.

  By default, Spark infers the schema from the data, however, sometimes we may need
  to define our own schema (column names and data types), especially while working with
  unstructured and semi-structured data, this article explains how to define simple,
  nested, and complex schemas with examples.

  While creating a Spark DataFrame we can specify the schema using StructType and
  StructField classes. we can also add nested struct StructType, ArrayType for
  arrays, and MapType for key-value pairs.

   */

  val spark = SparkSession
    .builder()
    .appName("Spark Schema Demo")
    .master("local")
    .getOrCreate()

  /*
   example demonstrates a very simple example of using
   StructType & StructField on DataFrame
   */
  val peopleSchema = StructType(
    Array(
      StructField("name", StringType, true),
      StructField("age", StringType, true)))

  import spark.implicits._
  val rdd = spark.sparkContext.textFile("src/main/resources/people.txt")
  val rowRDD = rdd.map(x =>
    Row(x.split(",")(0), x.split(",")(1)))

  spark.createDataFrame(rowRDD,peopleSchema).show()

  /*
  Nested struct Schema
   */

  val structureData = Seq(
    Row(Row("James", "", "Smith"), "36636", "M", 3100),
    Row(Row("Michael", "Rose", ""), "40288", "M", 4300),
    Row(Row("Robert", "", "Williams"), "42114", "M", 1400),
    Row(Row("Maria", "Anne", "Jones"), "39192", "F", 5500),
    Row(Row("Jen", "Mary", "Brown"), "", "F", -1)
  )

  val structureSchema = new StructType()
    .add("name", new StructType()
      .add("firstname", StringType)
      .add("middlename", StringType)
      .add("lastname", StringType))
    .add("id", StringType)
    .add("gender", StringType)
    .add("salary", IntegerType)

  spark.createDataFrame(
    spark.sparkContext.parallelize(structureData), structureSchema).show()

  /*
  Arrays & Map Columns
  Spark SQL also supports ArrayType and MapType to define the schema with array
  and map collections respectively. On the below example, column “hobbies” defined
  as ArrayType(StringType) and “properties” defined as MapType(StringType,StringType)
  meaning both key and value as String.
   */

  val arrayStructureData = Seq(
    Row(Row("James", "", "Smith"), List("Cricket", "Movies"), Map("hair" -> "black", "eye" -> "brown")),
    Row(Row("Michael", "Rose", ""), List("Tennis"), Map("hair" -> "brown", "eye" -> "black")),
    Row(Row("Robert", "", "Williams"), List("Cooking", "Football"), Map("hair" -> "red", "eye" -> "gray")),
    Row(Row("Maria", "Anne", "Jones"), null, Map("hair" -> "blond", "eye" -> "red")),
    Row(Row("Jen", "Mary", "Brown"), List("Blogging"), Map("white" -> "black", "eye" -> "black"))
  )

  val arrayStructureSchema = new StructType()
    .add("name", new StructType()
      .add("firstname", StringType)
      .add("middlename", StringType)
      .add("lastname", StringType))
    .add("hobbies", ArrayType(StringType))
    .add("properties", MapType(StringType, StringType))

  val df5 = spark.createDataFrame(
    spark.sparkContext.parallelize(arrayStructureData), arrayStructureSchema)
  df5.printSchema()
  df5.show()

  /*
  schema - Returns the schema of this Dataset.
  schema.sql - Return DDL

   */
  println(df5.schema.fieldNames.contains("properties"))
  println(df5.schema.sql)
}
