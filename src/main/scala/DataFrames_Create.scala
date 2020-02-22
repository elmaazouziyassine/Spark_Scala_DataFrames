import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object DataFrames_Create {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    // Create the SparkSession Object : spark
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Creating a DataSet")
      .getOrCreate()

    // To be able to use the $-notation & for implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    // Create a DataFrame from a list
    val myList = 0 to 100
    val myListDF = myList.toDF("number")
    myListDF.show(10)


    // Create a DataFrame for an external file
    val dataDF = spark.read.format("csv")
      .option("header", "true")
      .option("sep", ",")
      .option("inferSchema", "true")
      .load("src/datasets/flightsData2015")

    // Print the schema
    dataDF.printSchema()

    // Print 10 first elements
    dataDF.show(10)

  }
}
