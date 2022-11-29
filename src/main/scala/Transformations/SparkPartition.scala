package Transformations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SparkPartition extends App {

  val spark = SparkSession
    .builder
    .master("local[5]")
    .appName("SparkPartition")
    .getOrCreate()

  // Read the contents from JSON file and create Dataframe
  val df = spark.read.json("src/main/resources/people.json")
  //df.show()

  /*
  Spark partitioning is a way to split the data into multiple partitions so that
  you can execute transformations on multiple partitions in parallel which allows
  completing the job faster. You can also write partitioned data into a file system
  (multiple sub-directories) for faster reads by downstream systems.

  Notes:
  1. By default, Spark creates partitions that are equal to the number of CPU cores in the machine.
  2. Data of each partition resides in a single machine.
  3. Spark creates a task for each partition.
  4. Spark Shuffle operations move the data from one partition to other partitions.
  5. Partitioning is an expensive operation as it creates a data shuffle (Data could move between the nodes)
  6. By default, DataFrame shuffle operations create 200 partitions.

  Partition Filter and Pushed Filter:

  Partition Filter:
  Spark only grabs data from certain partitions and skips all of the irrelevant partitions.
  Data skipping allows for a big performance boost.
  These execute at the Physical Level.
  These only loads the relevant data in memory.

  Pushed Filter:
  The data is first loaded in memory and then filter is applied.
  Pushed Filter are useful when we load the data from a DB where in the filters are pushed
  to the DB Table only to fetch the relevant data.
   */
  //Partition Filter
  spark.read.csv("src/main/resources/partitionBy").filter("age=19").explain(true)

  //Pushed Filter
  spark.read.csv("src/main/resources/partitionBy").toDF("name","age").filter("name='Justin'").explain(true)
  Thread.sleep(1000000)



  /*
  repartition() and coalesce():
  repartition() is used to increase or decrease the RDD, DataFrame, Dataset partitions
  whereas the coalesce() is used to only decrease the number of partitions in an
  efficient way.

  Spark repartition() and coalesce() are very expensive operations as they shuffle the
  data across many partitions hence try to minimize repartition as much as possible.

  Repartition re-distributes the data(as shown below) from all partitions which is
  full shuffle leading to very expensive operation when dealing with billions and
  trillions of data.
  Even decreasing the partitions also results in moving data from all partitions.
  hence when you wanted to decrease the partition recommendation is to use coalesce()
  repartition() is a wider transformation that involves shuffling of the data.

  Coalesce() is used only to reduce the number of partitions. This is optimized or
  improved version of repartition() where the movement of the data across the partitions
  is lower using coalesce.
  If a larger number of partitions is requested, it will stay at the current number
  of partitions. Similar to coalesce defined on an RDD, this operation results in
  a narrow dependency, e.g. if you go from 1000 partitions to 100 partitions, there
  will not be a shuffle, instead each of the 100 new partitions will claim 10 of the
  current partitions.
  However, if you're doing a drastic coalesce, e.g. to numPartitions = 1, this may
  result in your computation taking place on fewer nodes than you like (e.g. one
  node in the case of numPartitions = 1). To avoid this, you can call repartition.
  This will add a shuffle step, but means the current upstream partitions will
  be executed in parallel (per whatever the current partitioning is).

  Unlike RDD, you can’t specify the partition/parallelism while creating DataFrame.
  DataFrame or Dataset by default uses the methods specified in Section 1 to determine
  the default partition and splits the data for parallelism.

  Calling groupBy(), union(), join() and similar functions on DataFrame results
  in shuffling data between multiple executors and even machines and finally repartitions
  data into 200 partitions by default. Spark default defines shuffling partition
  to 200 using spark.sql.shuffle.partitions configuration.
   */

  spark.conf.set("spark.sql.shuffle.partitions",8)
  println("partition: "+df.rdd.getNumPartitions)
  df.repartition(8)
  //repartition() can take int or column names as param to define how to perform the partitions.
  //If parameters are not specified, it uses the default number of partitions.
  df.repartition(6, col("age"))
  df.coalesce(2)

  /*
  Spark partitionBy() is a function of pyspark.sql.DataFrameWriter class that is
  used to partition based on one or multiple columns while writing DataFrame to Disk/File
  system. It creates a sub-directory for each unique value of the partition column.

  Partitions the output by the given columns on the file system. If specified, the
  output is laid out on the file system similar to Hive's partitioning scheme. As
  an example, when we partition a dataset by year and then month, the directory layout
  would look like:
  year=2016/month=01/
  year=2016/month=02/

  Partitioning is one of the most widely used techniques to optimize physical data
  layout. It provides a coarse-grained index for skipping unnecessary data reads
  when queries have predicates on the partitioned columns. In order for partitioning to
  work well, the number of distinct values in each column should typically be less than
  tens of thousands.

  This is applicable for all file-based data sources (e.g. Parquet, JSON) starting
  with Spark 2.1.0.
   */

  df
    .write
    .partitionBy("age")
    .mode("overwrite")
    .csv("src/main/resources/partitionBy/")

  /*
  Creating disk level partitioning, speeds up further data reading when you filter by
  partitioning column.
   */




}
