package SparkInAction

import SparkInAction.SparkSelect.columns
import org.apache.spark.{SPARK_VERSION, SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object SparkSessionDemo extends App {

  val spark = SparkSession
    .builder()
    .appName("Spark Session Demo")
    .master("local")
    .getOrCreate()

  //Returns a DataFrame with no rows or columns. it will ignore schema
  spark.emptyDataFrame.show()

  //Create DB
  spark.sql("create database IF NOT EXISTS test comment 'Test DB' LOCATION 'D:/Spark/DB/' WITH DBPROPERTIES (ID=001, Name='OJAS')")

  //Returns a list of databases available across all sessions.
  spark.catalog.listDatabases().show(false)

  //Create Schema for People CSV that needs to be loaded
  val peopleSchema = StructType(
                      Array(
                        StructField("name",StringType,true),
                        StructField("age",StringType,true),
                        StructField("job",StringType,true)))

  /*
  Catalog is the interface for managing a metastore (aka metadata catalog) of relational
  entities (e.g. database(s), tables, functions, table columns and temporary views).

  Interface through which the user may create, drop, alter or query underlying databases,
  tables, functions etc.

  Catalog is available using SparkSession.catalog property
   */

  //The the current database in order to create or access that tables in it.
  spark.catalog.setCurrentDatabase("test")

  /*
  Create Table using Scala API.
  (Scala-specific) Create a table based on the dataset in a data source, a schema and a set
  of options. Then, returns the corresponding DataFrame.
   */
  spark.catalog.createTable("people","csv",peopleSchema,Map("sep" -> ";","path" -> "file:/D:/Spark/people.csv"))

  //Returns a list of tables/views in the current database. This includes all
  // temporary views.
  spark.catalog.listTables().show()
  spark.sql("describe EXTENDED test.people ").show(false)
  spark.sql("select * from test.people").show()

  /*
  SparkSession vs SparkContext – Since earlier versions of Spark, SparkContext (JavaSparkContext
  for Java) is an entry point to Spark programming with RDD and to connect to Spark
  Cluster, Since Spark 2.0 SparkSession has been introduced and became an entry point
  to start programming with DataFrame and Dataset.

  SparkContext:
  When you do programming wither with Scala, PySpark or Java, first you need to create
  a SparkConf instance by assigning app name and setting master by using the SparkConf
  static methods setAppName() and setmaster() respectively and then pass SparkConf object
  as an argument to SparkContext constructor to create SparkContext.

  SparkSession:
  With Spark 2.0 a new class org.apache.spark.sql.SparkSession has been introduced to use
  which is a combined class for all different contexts we used to have prior to 2.0
  (SQLContext and HiveContext e.t.c) release hence SparkSession can be used in replace
  with SQLContext and HiveContext.

  Spark Session also includes all the APIs available in different contexts –
  1. Spark Context
  2. SQL Context
  3. Streaming Context
  4. Hive Context

  Why SparkSession is preferable over SparkContext in SparkSession Vs SparkContext
  battle is that SparkSession unifies all of Spark’s numerous contexts, removing the
  developer’s need to worry about generating separate contexts. Apart from this benefit,
  the Apache Spark developers have attempted to address the issue of numerous users
  sharing the same SparkContext.

  Only one SparkContext should be active per JVM. You must stop() the active SparkContext before creating a new one.
   */
  spark.stop()

  val conf = new SparkConf().setAppName("Spark Context").setMaster("local[1]")
  val sparkContext1 = new SparkContext(conf)
  val rdd = sparkContext1.textFile("src/main/resources/people.txt")
  rdd.foreach(println)



  sparkContext1.stop()

  /*
  SparkSession:
  You can create as many SparkSession as you want in a Spark application using
  either SparkSession.builder() or SparkSession.newSession(). Many Spark session
  objects are required when you wanted to keep Spark tables (relational entities)
  logically separated

  To create SparkSession in Scala or Python, you need to use the builder pattern
  method builder() and calling getOrCreate() method. If SparkSession already exists
  it returns otherwise creates a new SparkSession.

  builder() - method enables users to configure and create the SparkSession.
  master(), appName() and getOrCreate() are methods of builder.

  master() – If you are running it on the cluster you need to use your master
  name as an argument to master(). usually, it would be either yarn or mesos depends
  on your cluster setup
    1. Use local[x] when running in Standalone mode. x should be an integer
    value and should be greater than 0; this represents how many partitions it
    should create when using RDD, DataFrame, and Dataset. Ideally, x value should
    be the number of CPU cores you have.
    2. For standalone use spark://master:7077

  appName() – Sets a name to the Spark application that shows in the Spark web UI.
  If no application name is set, it sets a random name.
   */
  val spark1 = SparkSession
    .builder()
    .appName("Spark Session Demo1")
    .master("local")
    .getOrCreate()

  println("Spark Session Created")
  /*
  spark.executeCommand()

  Execute an arbitrary string command inside an external execution engine rather
  than Spark. This could be useful when user wants to execute some commands out
  of Spark. For example, executing custom DDL/DML command for JDBC, creating index
  for ElasticSearch, creating cores for Solr and so on.

  The command will be eagerly executed after this method is called and the returned
  DataFrame will contain the output of the command(if any).
   */

  /*
  Runtime configuration interface for Spark.
  This is the interface through which the user can get and set all Spark and Hadoop
  configurations that are relevant to Spark SQL. When getting the value of a config,
  this defaults to the value set in the underlying SparkContext
   */
  spark1.conf.getAll.foreach(println)

  /*
  Sets the given Spark runtime configuration property.
   */
  spark1.conf.set("spark.sql.files.maxRecordsPerFile",3)

  /*
  Ways to create Dataframe from RDD:
  1. toDF()
  2. createDataFrame

  Import SparkSession Implicits: import spark1.implicits._

  Scala allows you to import "dynamically" things into scope. You can also do something like that.
  Implicit methods available in Scala for converting common Scala objects into DataFrames.

  By importing implicits through import spark.implicits._ where spark is a SparkSession type object,
  the functionalities are imported implicitly.
   */

  import spark1.implicits._
  val rdd1 = spark1.sparkContext.textFile("src/main/resources/people.txt")
  rdd1.toDF.show()
  val rdd2 = rdd1.map(x =>
    (x.split(",")(0), x.split(",")(1).trim)).foreach(println)

  val peopleSchema1 = StructType(
    Array(
      StructField("name", StringType, true),
      StructField("age", StringType, true)))

  val rowRDD = rdd1.map( x =>
    Row(x.split(",")(0), x.split(",")(1)))

  /*
  Creates a DataFrame from an RDD containing Rows using the given schema. It
  is important to make sure that the structure of every Row of the provided RDD
  matches the provided schema. Otherwise, there will be runtime exception
   */
  spark1.createDataFrame(rowRDD,peopleSchema1).show()

  /*
  Datasets: discussed in SparkDatasets.scala
   */

  /*
  In order to use Hive with Spark, you need to enable it using the enableHiveSupport() method.

  Enables Hive support, including connectivity to a persistent Hive metastore,
  support for Hive serdes, and Hive user-defined functions.
   */
  SparkSession.builder().enableHiveSupport()

  Thread.sleep(100000)



}
