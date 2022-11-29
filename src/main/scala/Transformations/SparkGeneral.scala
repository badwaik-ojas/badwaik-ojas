package Transformations

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.Double.NaN

object SparkGeneral extends App {

  val spark = SparkSession
    .builder
    .master("local")
    .appName("SparkGeneral")
    .getOrCreate()

  val test = spark.emptyDataFrame
  test.createOrReplaceTempView("test")
  spark.sql("select date_format(to_date('21/11/2022','dd/MM/yyyy'),'dd-MM-yyyy') as t").show()

  val data = Seq(("James", "Smith", "USA", "CA"),
    ("Michael", "Rose", "USA", "NY"),
    ("Robert", "Williams", "USA", "CA"),
    ("Maria", "Jones", "USA", "FL"),
    ("James", null, "USA", "CA"),
    ("James", "Smith", "USA", "CA"),
    ("James", "Smith", null, "CA")
  )
  val columns = Seq("firstname", "lastname", "country", "state")

  import spark.implicits._

  val df = data.toDF(columns: _*)
  df.show()
  // head: Returns the first row.
  println("***** Printing Head")
  println(df.head().mkString(","))

  /*
  head(n): Returns the first n rows.
  This method should only be used if the resulting array is expected
   to be small, as all the data is loaded into the driver's memory
   */
  df.head(2)

  /*
  limit: Returns a new Dataset by taking the first n rows.
  The difference between this function and head is that head is an action
  and *** returns an array *** (by triggering query execution) while limit returns
  a new Dataset.
   */
  println("***** Printing Limit")
  println(df.limit(2).show())

  /*
  Cache and Persist:
  Spark Cache and persist are optimization techniques for iterative and
  interactive Spark applications to improve the performance of the jobs or
  applications.

  Though Spark provides computation 100 x times faster than traditional
  Map Reduce jobs, If you have not designed the jobs to reuse the repeating
  computations you will see degrade in performance when you are dealing
  with billions or trillions of data. Hence, we may need to look at the
  stages and use optimization techniques as one of the ways to improve
  performance.

  Cache: cache() method is used to cache the intermediate results of the
  transformation so that other transformation runs on top of cached will
  perform faster. Caching the result of the transformation is one of the
  optimization tricks to improve the performance of the long-running Spark
  applications/jobs

  Why do we need to cache the result?
  consider a scenario where we perform multiple PySpark transformations in a
  lineage, we use the caching technique. Caching the intermediate results
  significantly improves the performance of future transformations that uses
  the results of previous transformations.

  What happens when we cache?
  After calling the cache() function nothing happens with the data but the
  query plan is updated by the Cache Manager by adding a new operator.
  Acutal cache happens when you call Spark actions.

  — InMemoryRelation. On a high level this is just to store some information
  that would be used during the query execution later on when some action is
  called. Spark will look for the data in the caching layer and read it from
  there if it is available. If it doesn’t find the data in the caching layer
  (which happens for sure the first time the query runs), it will become
  responsible for getting the data there and it will use it immediately afterward.

  Monitoring cache:
  Spark automatically monitors cache usage on each node and drops out old
  data partitions in a least-recently-used (LRU) fashion. So least recently
  used will be removed first from cache.

  Uncache Table:
  If tables are cached by using createOrReplaceTempView() method, then you
  have to use different approach to remove it from cache.

  Both caching and persisting are used to save the Spark RDD, Dataframe, and
  Dataset’s. But, the difference is, RDD cache() method default saves it to
  memory (MEMORY_AND_DISK) whereas persist() method is used to store it to
  the user-defined storage level.

  When you persist a dataset, each node stores its partitioned data in memory
  and reuses them in other actions on that dataset. And Spark’s persisted
  data on nodes are fault-tolerant meaning if any partition of a Dataset is
  lost, it will automatically be recomputed using the original transformations
  that created it.

  Advantages:
  Cost efficient – Spark computations are very expensive hence reusing the computations are used to save cost.
  Time efficient – Reusing the repeated computations saves lots of time.
  Execution time – Saves execution time of the job and we can perform more jobs on the same cluster.

  Persist: Spark persist has two signature first signature doesn’t take any
  argument which by default saves it to MEMORY_AND_DISK storage level and
  the second signature which takes StorageLevel as an argument to store it
  to different storage levels.

  Different Storage Levels:
  Using the second signature you can save DataFrame/Dataset to One of the
  storage levels
  1. MEMORY_ONLY –
    This is the default behavior of the RDD cache() method and stores the
    RDD or DataFrame as deserialized objects to JVM memory. When there is no
    enough memory available it will not save DataFrame of some partitions
    and these will be re-computed as and when required. This takes more
    memory. but unlike RDD, this would be slower than MEMORY_AND_DISK level
    as it recomputes the unsaved partitions and recomputing the in-memory
    columnar representation of the underlying table is expensive
  2. MEMORY_AND_DISK - This is the default behavior of the DataFrame or
    Dataset. In this Storage Level, The DataFrame will be stored in JVM
    memory as a deserialized object. When required storage is greater than
    available memory, it stores some of the excess partitions into the disk
    and reads the data from the disk when required. It is slower as there
    is I/O involved.
  3. MEMORY_ONLY_SER - This is the same as MEMORY_ONLY but the difference
    being it stores RDD as serialized objects to JVM memory. It takes lesser
    memory (space-efficient) then MEMORY_ONLY as it saves objects as serialized
    and takes an additional few more CPU cycles in order to deserialize.
  4. MEMORY_AND_DISK_SER
  5. DISK_ONLY, MEMORY_ONLY_2
  6. MEMORY_AND_DISK_2

  Unpersist:
  We can also unpersist the persistence DataFrame or Dataset to remove from
  the memory or storage.

  Note on Persistence:
  - Spark automatically monitors every persist() and cache() calls you make
  and it checks usage on each node and drops persisted data if not used or
  using least-recently-used (LRU) algorithm. As discussed in one of the
  above section you can also manually remove using unpersist() method.

  - Spark caching and persistence is just one of the optimization techniques
  to improve the performance of Spark jobs.

  - On Spark UI, the Storage tab shows where partitions exist in memory or disk across the cluster.

  - Caching of Spark DataFrame or Dataset is a lazy operation, meaning a DataFrame will not be cached until you trigger an action.
   */

  //Cache
  df.cache()

  //Persist
  df.persist(StorageLevel.MEMORY_AND_DISK)

  /*
  createOrReplaceTempView():
  One of the main advantages of Apache Spark is working with SQL along with
  DataFrame/Dataset API. So if you are comfortable with SQL, you can create a
  temporary view on DataFrame/Dataset by using createOrReplaceTempView() and
  using SQL to select and manipulate the data.

  A Temporary view in Spark is similar to a real SQL table that contains rows
  and columns but the view is not materialized into files.

  How does this work in Spark?
  createOrReplaceTempView() in Spark creates a view only if not exist, if it
  exits it replaces the existing view with the new one. Spark SQL views are
  lazily evaluated meaning it does not persist in memory unless you cache
  the dataset by using the cache() method.

  Points to Note:
  - createOrReplaceTempView() is used when you wanted to store the table for a specific spark session.
  - Once created you can use it to run SQL queries.
  - These temporary views are session-scoped i.e. valid only that running spark session.
  - It can’t be shared between the sessions
  - These views will be dropped when the session ends unless you created it as Hive table.
  - Use saveAsTable() to materialize the contents of the DataFrame and create a pointer to the data in the metastore.
   */
  df.createOrReplaceTempView("df")
  spark.sql("select * from df").show()

  /*
  createOrReplaceGlobalTempView:
  Creates or replaces a global temporary view using the given name. The
  of this temporary view is tied to this Spark application.

  Global temporary view is cross-session. Its lifetime is the lifetime of
  the Spark application, i.e. it will be automatically dropped when the
  application terminates. It's tied to a system preserved database global_temp,
  and we must use the qualified name to refer a global temp view, e.g. SELECT
  * FROM global_temp.view1.
   */

  // Create Global Temp View Session
  df.createOrReplaceGlobalTempView("df_global")
  // Create New Session
  val sparknew = spark.newSession()
  println("SparkSession New Session:" + sparknew)
  // To query the global temp table use => global_temp view
  val sqlGlobalView = sparknew.sql("select * from global_temp.df_global")
  sqlGlobalView.show()

  //Prints the schema to the console in a nice tree format.
  df.printSchema(1)

  //Returns all column names as an array.
  val columns_1 = df.columns
  println(columns_1.mkString(","))

  /*
  Spark collect() and collectAsList() are action operation that is used to
  retrieve all the elements of the RDD/DataFrame/Dataset (from all nodes) to
  the driver node. We should use the collect() on smaller dataset usually after
  filter(), group(), count() e.t.c. Retrieving on larger dataset results in
  out of memory.

  collect():
  Action function is used to retrieve all elements from the dataset (RDD/DataFrame/Dataset)
  as a Array[Row] to the driver program.
  Note that like other DataFrame functions, collect() does not return a
  Dataframe instead, it returns data in an array to your driver. once the
  data is collected in an array, you can use scala language for further
  processing.

  Select Vs Collect:
  1. select() method on an RDD/DataFrame returns a new DataFrame that holds the
  columns that are selected whereas collect() returns the entire data set.
  2. select() is a transformation function whereas collect() is an action.

  collectAsList():
  Action function is similar to collect() but it returns Java util list.
   */
  df.collect().foreach(println)
  df.collectAsList().forEach(println)

  /*
   explain: Prints the physical plan to the console for debugging purposes.
   explain(true): Prints the plans (logical and physical) to the console for debugging purposes.
   explain(mode): Prints the plans (logical and physical) with a format specified by a given explain mode.
    simple: Print only a physical plan.
    extended: Print both logical and physical plans.
    codegen: Print a physical plan and generated codes if they are available.
    cost: Print a logical plan and statistics if they are available.
    formatted: Split explain output into two sections: a physical plan outline and node details.
   */
  spark.sql("select * from df").explain("codegen")
  df.explain()

  /*
  dropDuplicates vs distinct
  What is the difference between Spark distinct() vs dropDuplicates() methods?
  Both these methods are used to drop duplicate rows from the DataFrame and return
  DataFrame with unique values.

  The main difference is distinct() performs on all columns whereas dropDuplicates()
  is used on selected columns.
   */

  df.distinct().show()
  df.dropDuplicates().show()
  df.dropDuplicates(Seq("State")).show()

  /*
  Computes basic statistics for numeric and string columns, including count,
  mean, stddev, min, and max. If no columns are given, this function computes
  statistics for all numerical or string columns.

  This function is meant for exploratory data analysis, as we make no guarantee
  about the backward compatibility of the schema of the resulting Dataset. If
  you want to programmatically compute summary statistics, use the agg function
  instead.
   */
  df.describe().show()

  /*
  Computes specified statistics for numeric and string columns. Available
  statistics are:
  count
  mean
  stddev
  min
  max
  arbitrary approximate percentiles specified as a percentage (e.g. 75%)
  count_distinct
  approx_count_distinct
  If no statistics are given, this function computes count, mean, stddev,
  min, approximate quartiles (percentiles at 25%, 50%, and 75%), and max.

  This function is meant for exploratory data analysis, as we make no guarantee
  about the backward compatibility of the schema of the resulting Dataset.
   */
  df.summary().show()

  //Returns all column names and their data types as an array.
  df.dtypes

  /*
  na:
  Returns a DataFrameNaFunctions for working with missing data.

  na.fill:
  fills the null and NaN value with required details

  ns.drop:
  drops the rows having null value
  drops("any or all") the any rows containing null and if "all" then drops if all the row columns are null or NaN
   */
  df.na.fill("NOT NULL").show()
  df.na.drop().show()
  df.na.drop("all")
  df.na.replace("state",Map("CA" -> "CALIFORNIA" )).show()

}
