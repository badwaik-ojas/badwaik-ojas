import org.apache.spark.sql.SparkSession

import java.util.Properties

object SparkDataSourceDatabase {

  /**********************
   * THIS CODE IS NOT RUNNABLE *
   **********************/

  val spark = SparkSession
    .builder
    .master("local")
    .appName("SparkDataSourceDatabase")
    .getOrCreate()

  val jdbcDF = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql:dbserver")
    .option("dbtable", "schema.tablename")
    .option("user", "username")
    .option("password", "password")
    .load()

  // Using Connection Properties
  val connectionProperties = new Properties()
  connectionProperties.put("user", "username")
  connectionProperties.put("password", "password")
  val jdbcDF2 = spark.read
    .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

  // Specifying the custom data types of the read schema
  connectionProperties.put("customSchema", "id DECIMAL(38, 0), name STRING")
  val jdbcDF3 = spark.read
    .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)


  /*
  Spark SQL also includes a data source that can read data from other databases using JDBC.
  the results are returned as a DataFrame and they can easily be processed in Spark SQL or
  joined with other data sources.

  Options:
    url => The JDBC URL of the form jdbc:subprotocol:subname to connect to. The source-specific connection properties may be specified in the URL. e.g., jdbc:postgresql://localhost/test?user=fred&password=secret
    dbtable => The JDBC table that should be read from or written into.
      Note that when using it in the read path anything that is valid in a
      FROM clause of a SQL query can be used. For example, instead of a full
      table you could also use a subquery in parentheses.
      It is not allowed to specify dbtable and query options at the same time.
    query => A query that will be used to read data into Spark.
      The specified query will be parenthesized and used as a subquery in the FROM clause.
      Spark will also assign an alias to the subquery clause. As an example,
      spark will issue a query of the following form to the JDBC Source.
    driver => The class name of the JDBC driver to use to connect to this URL.
    partitionColumn, lowerBound, upperBound => These options must all be specified if any
      of them is specified. In addition, numPartitions must be specified.
      They describe how to partition the table when reading in parallel from
      multiple workers. partitionColumn must be a numeric, date, or timestamp
      column from the table in question.
    numPartitions => The maximum number of partitions that can be used for parallelism
      in table reading and writing. This also determines the maximum number of concurrent
      JDBC connections.
    fetchsize => The JDBC fetch size, which determines how many rows to fetch per round
      trip. This can help performance on JDBC drivers which default to low
      fetch size (e.g. Oracle with 10 rows).
    batchsize => The JDBC batch size, which determines how many rows to insert per round trip.
      This can help performance on JDBC drivers. This option applies only to
      writing.
    customSchema => The custom schema to use for reading data from JDBC connectors. For
      example, "id DECIMAL(38, 0), name STRING". You can also specify partial
      fields, and the others use the default type mapping. For example,
      "id DECIMAL(38, 0)". The column names should be identical to the
      corresponding column names of JDBC table.
   */
}
