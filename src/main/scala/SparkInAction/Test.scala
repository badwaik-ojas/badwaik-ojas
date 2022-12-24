package SparkInAction

import org.apache.spark.sql.SparkSession

object Test extends App {

  val spark = SparkSession
    .builder()
    .appName("Spark Session Demo1")
    .master("local")
    .config("spark.sql.legacy.timeParserPolicy","LEGACY")
    .getOrCreate()

  /*
  spark.sql.legacy.timeParserPolicy = LEGACY
  If you want to use the legacy format in a newer version of spark(>3),
  you need to set spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
  or spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY"), which will
  resolve the issue.
   */
  spark.sql("select to_date('5/12/2022','dd/MM/yyyy')").show()



}
