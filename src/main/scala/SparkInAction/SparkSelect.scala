package SparkInAction

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SparkSelect extends App {

  val spark = SparkSession
    .builder
    .master("local")
    .appName("SparkSelect")
    .getOrCreate()

  val data = Seq(("James", "Smith", "USA", "CA"),
    ("Michael", "Rose", "USA", "NY"),
    ("Robert", "Williams", "USA", "CA"),
    ("Maria", "Jones", "USA", "FL")
  )
  val columns = Seq("firstname", "lastname", "country", "state")

  import spark.implicits._

  val df = data.toDF(columns: _*)
  df.show()

  //Select a single or multiple rows
  df.select("firstname").show()
  df.select("firstname","lastname").show()

  //Get the list to columns in Dataframe
  println(df.columns.toSeq)
  val cols = df.columns.map( column => col(column))
  println("select all columns")
  df.select(cols:_*).show()

  //Select a column from a list => convert the list of columns in individual col type
  val colList = List("firstname","lastname")
  val columnList = colList.map(column => col(column))
  df.select(columnList:_*).show()

  //Select first N columns
  df.select(df.columns.slice(0,2).map(column => col(column)):_*)

  //Select columns by regex
  df.select(df.colRegex("`^.*name*`")).show()

  //Select columns that start with and end with
  df.select(df.columns.filter(column => column.startsWith("first")).map(column => col(column)):_*)

  //Prints the schema to the console in a nice tree format.
  df.printSchema()

  //Displays the top 20 rows of Dataset in a tabular form. Strings more than
  // 20 characters will be truncated, and all cells will be aligned right.
  df.show()

}
