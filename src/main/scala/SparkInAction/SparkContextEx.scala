package SparkInAction

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkFiles

object SparkContextEx extends App {

  val spark = SparkSession
    .builder()
    .appName("Spark Context")
    .master("local")
    .getOrCreate()

  /*
  Add a file to be downloaded with this Spark job on every node.
  If a file is added during execution, it will not be available until the next TaskSet
  starts.

  path – can be either a local file, a file in HDFS (or other Hadoop-supported filesystems),
  or an HTTP, HTTPS or FTP URI. To access the file in Spark jobs, use SparkFiles.get(fileName)
  to find its download location.
   */

  val sc = spark.sparkContext
  sc.addFile("C:\\sparkdatabase\\inventory\\student", true)
  val getFile = SparkFiles.get("student1.csv")

  /*
  setLogLevel:
  logLevel – The desired log level as a string. Valid log levels include: ALL
  , DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
   */
  sc.setLogLevel("ALL")

  /*
  applicationId:
  A unique identifier for the Spark application. Its format depends on the
  scheduler implementation. (i.e. in case of local spark app something like
  'local-1433865536131' in case of YARN something like 'application_1433865536131_34483'
  in case of MESOS something like 'driver-20170926223339-0001' )
   */

  println("AppID: "+sc.applicationId)

  val appID = sc.applicationAttemptId
  println("AppID: "+appID)



  Thread.sleep(100000)


}
