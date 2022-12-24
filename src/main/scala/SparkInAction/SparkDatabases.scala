package SparkInAction

import org.apache.spark.sql.SparkSession

object SparkDatabases extends App {

  val spark = SparkSession
    .builder()
    .appName("Spark Databases")
    .master("local")
    .getOrCreate()

  // Creates a database named `inventory`.
  spark.sql("CREATE DATABASE inventory")

  // Alters the database to set properties `Edited-by` and `Edit-date`
  spark.sql("ALTER DATABASE inventory SET DBPROPERTIES ('Edited-by' = 'John', 'Edit-date' = '01/01/2001')")

  // Verify that properties are set.
  spark.sql("DESCRIBE DATABASE EXTENDED inventory").show(false)

  // Alters the database to set a new location.
  spark.sql("ALTER DATABASE inventory SET LOCATION 'file:/C:/sparkdatabase/inventory'")

  spark.sql("DESCRIBE DATABASE EXTENDED inventory").show(false)

  spark.sql("use inventory")

  /*
  Create Table:
  The CREATE TABLE statement defines a new table using a Data Source.

  CREATE TABLE student (id INT, name STRING, age INT)
      USING CSV
      LOCATION 'file:/C:/sparkdatabase/inventory/student'
      TBLPROPERTIES (sep='|');
   */

  spark.sql("CREATE TABLE student (id INT, name STRING, age INT) USING CSV LOCATION 'file:///C:/sparkdatabase/inventory/student/'  ")

  spark.sql("describe table EXTENDED inventory.student").show(40,false)
  spark.sql("select * from inventory.student").show(false)

  /*
  Hive Format:
   */
  spark.sql("CREATE EXTERNAL TABLE student1 (id INT, name STRING)    PARTITIONED BY (age INT)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ESCAPED BY '\\\\'  STORED AS TEXTFILE LOCATION 'file:///C:/sparkdatabase/inventory/student/'")
  spark.sql("select * from inventory.student1").show()


}
