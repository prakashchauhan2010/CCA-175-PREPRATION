package udf

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import common.ConnectionUtil

object UDF extends App{
  
  /**
   * User-defined functions (UDFs) are a key feature of most SQL environments to extend the system’s built-in functionality.  
   * UDFs allow developers to enable new functions in higher level languages such as SQL by abstracting their lower level language implementations.  
   * Apache Spark is no exception, and offers a wide range of options for integrating UDFs with Spark SQL workflows.
   */
  
  val spark = ConnectionUtil.spark
  import spark.implicits._
  val sc = ConnectionUtil.sc
  spark.sparkContext.setLogLevel("ERROR")
  
  val inputFile = "data-files\\udf.json"
  val data = spark.read.json(inputFile)
  data.createOrReplaceTempView("city_data")
  data.select("city","avgLow","avgHigh").show()
  
  // Convert temperatures in the following JSON data from Celsius to Fahrenheit:
  spark.udf.register("CTOF", (celcius: Double) => ((celcius * 9.0 / 5.0) + 32.0))
  
  spark.sql("SELECT city, CTOF(avgLow) AS avgLowF, CTOF(avgHigh) AS avgHighF FROM city_data").show()
  
  
  
}