package SparkInAction

import org.apache.spark.sql.SparkSession

object SparkSQL extends App {

  val spark = SparkSession
    .builder()
    .appName("Spark SQL")
    .master("local[*]")
    .getOrCreate()

  /*
  Set a human readable description of the current job.
  Able to distinguish between various stages in the Stages Tab.
   */

  spark.sparkContext.setJobDescription("Date")

  /* Spark SQL Date Function:
  default format:  yyyy-MM-dd
  If a String, it should be in a format defined above.
  Convert date into a specific date format using date_format().
   */

  spark.sql("select current_date() as date," +
    "date_format(current_date(),'dd/MM/yyyy') as dateformat," +
    "weekofyear(current_date()) as weekno," +
    "unix_timestamp() as unixtmsmp").show(false)

  spark.sparkContext.setJobDescription("Timestamp")

  /* Spark SQL Timestamp Function:
  default format: yyyy-MM-dd HH:mm:ss.SSSS
  If a String, it should be in a format defined above.
   */

  spark.sql("select current_timestamp() as ctmpstmp, from_unixtime(unix_timestamp()) as tmstmp").show()

  spark.sparkContext.setJobDescription("Agg")

  /*
  array_agg: Collects and returns a list of non-unique elements.
   */

  spark.sql("SELECT array_agg(col) FROM VALUES (1), (2), (1) AS tab(col)").show()

  spark.sparkContext.setJobDescription("Dense Rank")

  /*
  Windows Function:
  dense_rank: Computes the rank of a value in a group of values. The result is
  one plus the previously assigned rank value. Unlike the function rank, dense_rank
  will not produce gaps in the ranking sequence.
   */

  val data = Seq(("James", "Smith", "USA", "CA"),
    ("Michael", "Rose", "USA", "NY"),
    ("Robert", "Williams", "USA", "CA"),
    ("Maria", "Jones", "USA", "FL"),
    ("James", null, "Canada", "CA"),
    ("James", "Smith", "Mexico", "CA"),
    ("James", "Smith", "USA", "CA"),
    ("James", "Smith", "India", "CA"),
    ("James", "Smith", "Vietnam", "CA")
  )

  val columns = Seq("firstname", "lastname", "country", "state")

  import spark.implicits._

  val df = data.toDF(columns: _*)
  df.createOrReplaceTempView("emp")

  spark.sql("" +
    "select firstname, country, dense_rank(country) over (partition by firstname order by country) as rank from emp" +
    "").show()

  /*
  Lag:
  Returns the value of `input` at the `offset`th row before the current row in
  the window. The default value of `offset` is 1 and the default value of `default`
  is null. If the value of `input` at the `offset`th row is null, null is returned.
  If there is no such offset row (e.g., when the offset is 1, the first row of the
  window does not have any previous row), `default` is returned.
   */

  spark.sql("" +
    "select firstname, country, lag(country) over (partition by firstname order by country) as lag from emp" +
    "").show()

  /*
  Lead:
  Returns the value of `input` at the `offset`th row after the current row in the
  window. The default value of `offset` is 1 and the default value of `default`
  is null. If the value of `input` at the `offset`th row is null, null is returned.
  If there is no such an offset row (e.g., when the offset is 1, the last row of
  the window does not have any subsequent row), `default` is returned.
   */

  spark.sql("" +
    "select firstname, country, lead(country) over (partition by firstname order by country) as lead from emp" +
    "").show()

  /*
  nth_value:
  Returns the value of `input` at the row that is the `offset`th row from beginning
  of the window frame. Offset starts at 1. If ignoreNulls=true, we will skip nulls
  when finding the `offset`th row. Otherwise, every row counts for the `offset`.
  If there is no such an `offset`th row (e.g., when the offset is 10, size of the
  window frame is less than 10), null is returned.
   */

  spark.sql("" +
    "select firstname, country, nth_value(country, 2) over (partition by firstname order by country) as nth_value from emp" +
    "").show()

  /*
  Rank:
  Computes the rank of a value in a group of values. The result is one plus the
  number of rows preceding or equal to the current row in the ordering of the
  partition. The values will produce gaps in the sequence.

  rank and dense_rank are similar to row_number, but when there are ties, they
  will give the same value to the tied values. rank will keep the ranking, so the
  numbering may go 1, 2, 2, 4 etc, whereas dense_rank will never give any gaps
   */

  spark.sql("" +
    "select firstname, country, rank(country) over (partition by firstname order by country) as rank from emp" +
    "").show()

  /*
  row_number:
  Assigns a unique, sequential number to each row, starting with one, according to the
  ordering of rows within the window partition.
   */

  spark.sql("" +
    "select firstname, country, row_number() over (partition by firstname order by country) as row_number from emp" +
    "").show()

  //Thread.sleep(1000000)
}
