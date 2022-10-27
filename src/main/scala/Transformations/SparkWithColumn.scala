package Transformations

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object SparkWithColumn extends App {

  val spark = SparkSession
    .builder
    .master("local")
    .appName("SparkWithColumn")
    .getOrCreate()

  /*
  Spark withColumn() is a transformation function of DataFrame that is used
    to manipulate the column values of all rows or selected rows on DataFrame.

  withColumn() function returns a new Spark DataFrame after performing
    operations like adding a new column, update the value of an existing
    column, derive a new column from an existing column, and many more.

  withColumn(colName : String, col : Column) : DataFrame

  Notes: This method introduces a projection internally. Therefore, calling
    it multiple times, for instance, via loops in order to add multiple columns
    can generate big plans which can cause performance issues and even StackOverflowException.
    To avoid this, use select() with the multiple columns at once.


   */

  val data = Seq(Row(Row("James;", "", "Smith"), "36636", "M", "3000"),
    Row(Row("Michael", "Rose", ""), "40288", "M", "4000"),
    Row(Row("Robert", "", "Williams"), "42114", "M", "4000"),
    Row(Row("Maria", "Anne", "Jones"), "39192", "F", "4000"),
    Row(Row("Jen", "Mary", "Brown"), "", "F", "-1")
  )

  val schema = new StructType()
    .add("name", new StructType()
      .add("firstname", StringType)
      .add("middlename", StringType)
      .add("lastname", StringType))
    .add("dob", StringType)
    .add("gender", StringType)
    .add("salary", StringType)

  val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

  /*
  Add a New Column in a Dataframe

  Spark SQL functions lit() and typedLit() are used to add a new constant column to
  DataFrame by assigning a literal or constant value.
   */

  df.withColumn("Country", lit("USA")).show()

  /*
  Change value of existing column
  Change Datatype of existing column
   */

  df.withColumn("salary",col("salary")*1000).show()
  df.withColumn("salary",col("salary").cast("Integer")).printSchema()

  //Print the sub column
  df.withColumn("firstname",col("name.firstname")).show()

  /*
  Derive a new column from existing column
   */
  df.withColumn("salary_new",col("salary")*1000).show()

  /*
  When multiple columns are involved. It is not suggestible to chain
  withColumn() function as it leads into performance issue and recommends to
  use select() after creating a temporary view on DataFrame
   */

  df.createOrReplaceTempView("PERSON")
  spark.sql("SELECT salary*100 as salary, salary*-1 as CopiedColumn, 'USA' as country FROM PERSON").show()

  /*
  Rename a column
   */
  df.withColumnRenamed("gender","sex").show()

  /*
  Drop a Column
   */
  df.drop("gender").show()

  /*
  Split a column into Multiple Columns
   */

}
