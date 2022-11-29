package SparkInAction

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkAggregations extends App {

  val spark = SparkSession
    .builder
    .master("local")
    .appName("SparkCheckpoint")
    .getOrCreate()

  val data = Seq(("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  )
  val columns = Seq("employee_name", "department", "salary")

  import spark.implicits._

  val df = data.toDF(columns: _*)
  df.show()

  /*
  Aggregations:
  Spark provides built-in standard Aggregate functions defines in DataFrame API,
  these come in handy when we need to make aggregate operations on DataFrame columns.
  Aggregate functions operate on a group of rows and calculate a single return value
  for every group.

  All these aggregate functions accept input as, Column type or column name in a
  string and several other arguments based on the function and return Column type.

  Below are the list of functions:
  approx_count_distinct
  avg
  collect_list
  collect_set
  countDistinct
  count
  grouping
  first
  last
  kurtosis
  max
  min
  mean
  skewness
  stddev
  stddev_samp
  stddev_pop
  sum
  sumDistinct
  variance, var_samp, var_pop
   */

  /*
  approx_count_distinct: Returns the count of distinct items in a group.
   */
  df.select(approx_count_distinct("salary").as("approx_count_distinct")).show()

  /*
  avg() function returns the average of values in the input column.
   */
  df.select(avg("salary").as("avg_salary")).show()

  /*
  collect_list() function returns all values from an input column with duplicates.
   */

  df.select(collect_list("salary").as("collect_list_salary")).show(false)

  /*
  countDistinct() function returns the number of distinct elements in a columns
   */
  df.select(countDistinct("department", "salary")).show()

  /*
  count() function returns number of elements in a column.
   */
  df.select(count("department")).show()

  /*
  first() function returns the first element in a column when ignoreNulls is set to true, it returns the first non-null element.
   */
  df.select(first("salary")).show()

  /*
  last() function returns the last element in a column. when ignoreNulls is set to true, it returns the last non-null element.
   */
  df.select(last("salary")).show()

  /*
  max() function returns the maximum value in a column.
   */
  df.select(max("salary")).show()

  /*
  min() function returns the minimum value in a column.
   */
  df.select(min("salary")).show()

  /*
  mean() function returns the average of the values in a column. Alias for Avg
   */
  df.select(mean("salary")).show()

  /*
  skewness() function returns the skewness of the values in a group.
   */
  df.select(skewness("salary")).show()

  /*
  Standard deviation in statistics, typically denoted by σ, is a measure of variation
  or dispersion (refers to a distribution's extent of stretching or squeezing) between
  values in a set of data. The lower the standard deviation, the closer the data points
  tend to be to the mean (or expected value), μ. Conversely, a higher standard deviation
  indicates a wider range of values. Similar to other mathematical and statistical concepts,
  there are many different situations in which standard deviation can be used, and thus
  many different equations. In addition to expressing population variability, the
  standard deviation is also often used to measure statistical results such as the
  margin of error. When used in this manner, standard deviation is often called the standard
  error of the mean, or standard error of the estimate with regard to a mean.

  stddev() alias for stddev_samp.
  stddev_samp() function returns the sample standard deviation of values in a column.
  stddev_pop() function returns the population standard deviation of the values in a column.
   */

  df.select(stddev("salary"), stddev_samp("salary"),stddev_pop ("salary")).show(false)

  /*
  sum() function Returns the sum of all values in a column.
  sumDistinct() function returns the sum of all distinct values in a column.
   */

  df.select(sum("salary")).show()
  df.select(sumDistinct("salary")).show()

  /*
  The term variance refers to a statistical measurement of the spread between numbers
  in a data set. More specifically, variance measures how far each number in the set
  is from the mean (average), and thus from every other number in the set. Variance
  is often depicted by this symbol: σ2. It is used by both analysts and traders
  to determine volatility and market security.

  variance() alias for var_samp
  var_samp() function returns the unbiased variance of the values in a column.
  var_pop() function returns the population variance of the values in a column.
   */

  df.select(variance("salary"), var_samp("salary"), var_pop("salary")).show()

  /*
  agg:
  Compute aggregates by specifying a series of aggregate columns. Note that this
  function by default retains the grouping columns in its output. To not retain grouping
  columns, set spark.sql.retainGroupColumns to false.

  Spark Groupby Agg is used to calculate more than one aggregate (multiple aggregates) at a
  time on grouped DataFrame. So to perform the agg, first, you need to perform the groupBy()
  on DataFrame which groups the records based on single or multiple column values, and then
  do the agg() to get the aggregate for each group.

  # Syntax
  GroupedData.agg(*exprs)
   */

  df.groupBy("department","salary").agg(count("*")).show()
  df.groupBy("department")
  .agg(sum("salary").alias("sum_salary"),
    avg ("salary").alias("avg_salary"),
    sum ("bonus").alias("sum_bonus"),
    max ("bonus").alias("max_bonus"))
  .show(false)

}
