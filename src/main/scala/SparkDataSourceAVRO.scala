import org.apache.spark.sql.SparkSession

object SparkDataSourceAVRO extends App {

  val spark = SparkSession
    .builder
    .master("local")
    .appName("SparkDataSourceAVRO")
    .getOrCreate()

  /*
  Need to add the dependency to start working with AVRO file format
  libraryDependencies += "org.apache.spark" %% "spark-avro" % "3.2.1"
  Since spark-avro module is external, there is no .avro API
   */

  val userDF = spark.read.format("avro").load("src/main/resources/users.avro")
  userDF.show()

  /*
  to_avro() and from_avro()

  The Avro package provides function to_avro to encode a column as binary in
  Avro format, and from_avro() to decode Avro binary data into a column.
  Both functions transform one column to another column, and the input/output
  SQL data type can be a complex type or a primitive type.
   */

}
