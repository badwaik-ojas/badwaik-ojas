package Transformations

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._

object SparkGroupBy extends App {

  val spark = SparkSession
    .builder
    .master("local")
    .appName("SparkGroupBy")
    .getOrCreate()

  /*
  Similar to SQL “GROUP BY” clause, Spark groupBy() function is used to
  collect the identical data into groups on DataFrame/Dataset and perform aggregate
  functions on the grouped data.

  When we perform groupBy() on Spark Dataframe, it returns RelationalGroupedDataset
  object which contains below aggregate functions.

  count() - Returns the count of rows for each group.
  mean() - Returns the mean of values for each group.
  max() - Returns the maximum of values for each group.
  min() - Returns the minimum of values for each group.
  sum() - Returns the total for values for each group.
  avg() - Returns the average for values for each group.
  agg() - Using agg() function, we can calculate more than one aggregate at a time.
  pivot() - This function is used to Pivot the DataFrame
  */

  import spark.implicits._

  val simpleData = Seq(("James", "Sales", "NY", 90000, 34, 10000),
    ("Michael", "Sales", "NY", 86000, 56, 20000),
    ("Robert", "Sales", "CA", 81000, 30, 23000),
    ("Maria", "Finance", "CA", 90000, 24, 23000),
    ("Raman", "Finance", "CA", 99000, 40, 24000),
    ("Scott", "Finance", "NY", 83000, 36, 19000),
    ("Jen", "Finance", "NY", 79000, 53, 15000),
    ("Jeff", "Marketing", "CA", 80000, 25, 18000),
    ("Kumar", "Marketing", "NY", 91000, 50, 21000)
  )
  val df = simpleData.toDF("employee_name", "department", "state", "salary", "age", "bonus")
  df.show()

  df.groupBy("department").sum("salary").show(false)
  df.groupBy("department").count().show()
  df.groupBy("department").min("salary").show()
  df.groupBy("department").max("salary").show()
  df.groupBy("department").avg("salary").show()
  df.groupBy("department").mean("salary").show()

  //Groupby on Multiple Columns
  df.groupBy("department", "state").sum("salary", "bonus").show()

  //Running more than one aggregate function
  df.groupBy("department").agg(sum("salary").as("sum"),avg("salary").as("avg")).show()

  //Using Filter with GroupBy Clause
  df.groupBy("department").agg(sum("salary").as("sum"),avg("salary").as("avg")).where(col("sum")>= "300000").show()

  /*
  Spark pivot() function is used to pivot/rotate the data from one DataFrame/Dataset
  column into multiple columns (transform row to column) and unpivot is used
  to transform it back (transform columns to rows).

  Pivot() is an aggregation where one of the grouping columns values transposed into individual columns with distinct data.
   */

  val data = Seq(("Banana", 1000, "USA"), ("Carrots", 1500, "USA"), ("Beans", 1600, "USA"),
    ("Orange", 2000, "USA"), ("Orange", 2000, "USA"), ("Banana", 400, "China"),
    ("Carrots", 1200, "China"), ("Beans", 1500, "China"), ("Orange", 4000, "China"),
    ("Banana", 2000, "Canada"), ("Carrots", 2000, "Canada"), ("Beans", 2000, "Mexico"))

  val df1 = data.toDF("Product", "Amount", "Country")
  df1.show()

  val pivotDF = df1.groupBy("Product").pivot("Country").sum("Amount")
  pivotDF.show()

}
