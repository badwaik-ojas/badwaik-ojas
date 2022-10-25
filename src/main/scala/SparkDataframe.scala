import org.apache.spark.sql.SparkSession

object SparkDataframe extends App {

  val spark = SparkSession
    .builder
    .master("local")
    .appName("SparkDataframe")
    .getOrCreate()

  // Read the contents from JSON file and create Dataframe
  val createDF = spark.read.json("src/main/resources/people.json")

  // Displays the content of the DataFrame on Console
  createDF.show()

  // Print the schema in a tree format
  createDF.printSchema()

  // Select => use this to select only the specific columns
  createDF.select("age", "name").show()

  // Filter => use filter to get only the required data from DF
  createDF.filter("age > 25").show()

  // groupBy => group people by age and then count
  createDF.groupBy("age").count().show()

  // Register the DataFrame as a SQL temporary view
  createDF.createOrReplaceTempView("people")

  val sqlDF = spark.sql("select * from people")

  sqlDF.show()

  println("SparkSession Old Session:"+spark)

  // Register the DataFrame as a SQL Global temporary view
  createDF.createOrReplaceGlobalTempView("people_global_view")

  // Create New Session
  val sparknew = spark.newSession()

  println("SparkSession New Session:"+sparknew)

  // To query the global temp table use => global_temp view
  val sqlGlobalView = sparknew.sql("select * from global_temp.people_global_view")

  sqlGlobalView.show()

}
