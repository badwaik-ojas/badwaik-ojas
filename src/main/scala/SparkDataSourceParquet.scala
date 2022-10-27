import org.apache.spark.sql.SparkSession

object SparkDataSourceParquet extends App {

  val spark = SparkSession
    .builder
    .master("local")
    .appName("SparkDataSourceParquet")
    .getOrCreate()

  /*
  Parquet is a columnar format that is supported by many other data
  processing systems. Spark SQL provides support for both reading and writing
  Parquet files that automatically preserves the schema of the original data.

  When reading Parquet files, all columns are automatically converted to be
  nullable for compatibility reasons.
   */

  /*
  Partition Discovery:

  Table partitioning is a common optimization approach used in systems like
  Hive. In a partitioned table, data are usually stored in different
  directories, with partitioning column values encoded in the path of each
  partition directory.

  All built-in file sources (including Text/CSV/JSON/ORC/Parquet) are able
  to discover and infer partitioning information automatically. For example
  , we can store all our previously used population data into a partitioned
  table using the following directory structure, with two extra columns,
  gender and country as partitioning columns

  path
  └── to
      └── table
          ├── gender=male
          │   ├── ...
          │   │
          │   ├── country=US
          │   │   └── data.parquet
          │   ├── country=CN
          │   │   └── data.parquet
          │   └── ...
          └── gender=female.........

  By passing path/to/table to either SparkSession.read.parquet or
  SparkSession.read.load, Spark SQL will automatically extract the
  partitioning information from the paths.

  Notice that the data types of the partitioning columns are automatically
  inferred. Currently, numeric data types, date, timestamp and string type
  are supported. Sometimes users may not want to automatically infer the
  data types of the partitioning columns. For these use cases, the automatic
  type inference can be configured by spark.sql.sources.partitionColumnTypeInference.enabled,
  which is default to true. When type inference is disabled, string type will
  be used for the partitioning columns.

  Starting from Spark 1.6.0, partition discovery only finds partitions under
  the given paths by default. For the above example, if users pass
  path/to/table/gender=male to either SparkSession.read.parquet or
  SparkSession.read.load, gender will not be considered as a partitioning
  column. The users need to specify the base path that partition discovery
  should start with, they can set basePath in the data source options. For
  example, when path/to/table/gender=male is the path of the data and users
  set basePath to path/to/table/, gender will be a partitioning column.
   */

  /*
  Metadata Refreshing
  Spark SQL caches Parquet metadata for better performance.
  When Hive metastore Parquet table conversion is enabled, metadata of those
  converted tables are also cached.
  If these tables are updated by Hive or other external tools, you need to
  refresh them manually to ensure consistent metadata.
   */

  /*
  spark.sql.parquet.compression.codec = snappy
  */
  import spark.implicits._
  val usersDF = spark.read.load("src/main/resources/users.parquet")
  usersDF.show()

  //Trying different compression technique
  usersDF
    .write
    .mode("overwrite")
    .option("compression","gzip")
    .save("src/main/resources/users.parquet.gzip")

  usersDF
    .write
    .mode("overwrite")
    .option("compression", "snappy")
    .save("src/main/resources/users.parquet.snappy")

}
