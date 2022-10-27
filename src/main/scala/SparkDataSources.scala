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

  /* Bucketing => For file-based data source, it is also possible to bucket
   and sort or partition the output.
   Bucketing and sorting are applicable only to persistent tables

   Buckets the output by the given columns. If specified, the output is
   laid out on the file system similar to Hive's bucketing scheme,
   but with a different bucket hash function and is not compatible with
   Hive's bucketing.

   This is applicable for all file-based data sources
   */

  println("Bucketing")
  peopleDF
    .write
    .mode("overwrite")
    .bucketBy(42, "name")
    .sortBy("age")
    .option("path","/src/main/resources/persistent_bucket")
    .saveAsTable("people_bucketed")

  /*
  while partitioning can be used with both save and saveAsTable when using the Dataset APIs

  Partitions the output by the given columns on the file system.
  If specified, the output is laid out on the file system similar to Hive's
  partitioning scheme.

  As an example, when we partition a dataset by year and then month,
  the directory layout would look like:

  year=2016/month=01/
  year=2016/month=02/
  Partitioning is one of the most widely used techniques to optimize physical
  data layout. It provides a coarse-grained index for skipping unnecessary
  data reads when queries have predicates on the partitioned columns.

  In order for partitioning to work well, the number of distinct values in
  each column should typically be less than tens of thousands.
   */

  println("Partitioning")
  usersDF
    .write
    .mode("overwrite")
    .partitionBy("favorite_color")
    .format("parquet")
    .save("src/main/resources/namesPartByColor.parquet")

  /*
  It is possible to use both partitioning and bucketing for a single table
   */

  println("Bucketing & Partitioning")
  usersDF
    .write
    .mode("overwrite")
    .partitionBy("favorite_color")
    .bucketBy(42, "name")
    .option("path","/src/main/resources/persistent_bucket_partition")
    .saveAsTable("users_partitioned_bucketed")

  /*
  partitionBy creates a directory structure as described in the Partition
  Discovery section.
  Thus, it has limited applicability to columns with high cardinality. In
  contrast bucketBy distributes data across a fixed number of buckets
  and can be used when the number of unique values is unbounded.
   */

}
