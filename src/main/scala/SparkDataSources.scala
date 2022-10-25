import SparkDatasets.spark
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkDataSources extends App {

  val spark = SparkSession
    .builder
    .master("local")
    .appName("SparkDataSources")
    .getOrCreate()



  /* This code deals with different Data Sources we need to deal with in Spark
     How to load Data from Different Data sources and Save the data in Different
     Data sources.
     What is the default Data Source while performing the above activity => parquet */

  /* The default data source (parquet unless otherwise configured by
   spark.sql.sources.default) will be used for all operations. */

  /*
  Parquet Files
  Parquet is a columnar format that is supported by many other data processing
  systems.
  Spark SQL provides support for both reading and writing Parquet files that
  automatically preserves the schema of the original data.
  When writing Parquet files, all columns are automatically converted to be
  nullable for compatibility reasons.
   */

  val usersDF = spark.read.load("src/main/resources/users.parquet")
  usersDF.show()
  /*
  Mode => Specifies the behavior when data or table already exists. Options include:
  SaveMode.Overwrite: overwrite the existing data.
  SaveMode.Append: append the data.
  SaveMode.Ignore: ignore the operation (i.e. no-op).
  SaveMode.ErrorIfExists: throw an exception at runtime.
  The default option is ErrorIfExists.
   */
  usersDF.write.mode(SaveMode.Overwrite).save("src/main/resources/users_SaveDemo.parquet")

  /*
  Manually Specifying Options
  You can also manually specify the data source that will be used along with
   any extra options that you would like to pass to the data source.
   Data sources are specified by their fully qualified name
   (i.e., org.apache.spark.sql.parquet), but for built-in sources you can
   also use their short names (json, parquet, jdbc, orc, libsvm, csv, text).
   DataFrames loaded from any data source type can be converted into other types
   using this syntax.
   */

  val peopleDF = spark.read.format("json").load("src/main/resources/people.json")
  peopleDF.show()
  peopleDF.select("name", "age").write.mode("overwrite").format("parquet").save("src/main/resources/people_SaveDemo.parquet")

  /* Run SQL on Files directly */

  val sqlDF = spark.sql("SELECT * FROM parquet.`src/main/resources/users.parquet`")
  sqlDF.show()

  /*
  The extra options are also used during write operation.
  For example, you can control bloom filters and dictionary encodings for
  ORC data sources. The following ORC example will create bloom filter and
  use dictionary encoding only for favorite_color.
   */

  /*
  Saving to Persistent Tables
  DataFrames can also be saved as persistent tables into Hive metastore using
  the saveAsTable command.
  Notice that an existing Hive deployment is not necessary to use this feature.
  Spark will create a default local Hive metastore (using Derby) for you.

  Unlike the createOrReplaceTempView command, saveAsTable will materialize the
  contents of the DataFrame and create a pointer to the data in the Hive metastore.

  Persistent tables will still exist even after your Spark program has restarted,
  as long as you maintain your connection to the same metastore.

  A DataFrame for a persistent table can be created by calling the table method
  on a SparkSession with the name of the table.
   */

  /* Saving users.parquet using the persistent table option in Spark */

  usersDF
    .write
    .mode(SaveMode.Overwrite)
    .option("path","/src/main/resources/persistent")
    .saveAsTable("users")

  //Creating a new
  val sparkNewSession = spark.newSession()

  /*Trying to fetch the Persistent table created in OLD session
   and fetching it.*/
  val testDF = sparkNewSession.table("users").show()


}
