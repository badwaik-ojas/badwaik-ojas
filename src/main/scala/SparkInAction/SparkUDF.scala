package SparkInAction

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

object SparkUDF extends App {

  val spark = SparkSession
    .builder()
    .appName("Spark UDF")
    .master("local")
    .getOrCreate()

  /*
    Spark SQL UDF (a.k.a User Defined Function) is the most useful feature of Spark
    SQL & DataFrame which extends the Spark build in capabilities

    UDF’s are used to extend the functions of the framework and re-use this function
    on several DataFrame. For example if you wanted to convert the every first letter
    of a word in a sentence to capital case, spark build-in features does’t have this
    function hence you can create it as UDF and reuse this as needed on many Data
    Frames. UDF’s are once created they can be re-use on several DataFrame’s and
    SQL expressions.

    When you creating UDF’s you need to design them very carefully otherwise you
    will come across performance issues.

    UDF’s are a black box to Spark hence it can’t apply optimization and you will
    lose all the optimization Spark does on Dataframe/Dataset. When possible you
    should use Spark SQL built-in functions as these functions provide optimization.

    UDF’s are error-prone when not designed carefully. for example, when you have a
    column that contains the value null on some records and not handling null inside
    a UDF function returns below error.
   */

  import spark.implicits._

  val columns = Seq("Seqno", "Quote")
  val data = Seq(("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy.")
  )
  val df = data.toDF(columns: _*)
  df.show(false)

  //Create a Function
  val convertCase = (strQuote: String) => {
    val arr = strQuote.split(" ")
    arr.map(f => f.substring(0, 1).toUpperCase + f.substring(1, f.length)).mkString(" ")
  }

  //Use the above function

  val convertUDF = udf(convertCase)
  df.select(col("Seqno"),
    convertUDF(col("Quote")).as("Quote")).show(false)

  //Registering Spark UDF to use it on SQL

  spark.udf.register("convertUDF", convertCase)
  df.createOrReplaceTempView("QUOTE_TABLE")
  spark.sql("select Seqno, convertUDF(Quote) from QUOTE_TABLE").show(false)

}
