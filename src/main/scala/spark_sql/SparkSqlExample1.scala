package spark_sql
import org.apache.spark.sql._
import common._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.log4j.{ Level, Logger }

object SparkSqlExample1 extends App {
  val spark = ConnectionUtil.spark
  import spark.implicits._
  val sc = ConnectionUtil.sc
  spark.sparkContext.setLogLevel("ERROR")

  case class Person(name: String, age: Long)
/*  val personDF = spark.read.json("data-files//people.json")
  val personJsonDS = personDF.as[Person]
  personJsonDS.show()
  personJsonDS.groupBy($"age").count().show()
  personJsonDS.filter($"age" < 25).show()
  personJsonDS.filter($"name".startsWith("J")).show()*/
  
  val schema = StructType(Array(StructField("name", StringType, true), StructField("age", IntegerType, true)))
  val personCSVDf = spark.read.schema(schema).csv("data-files//people.txt")
  val personCSVDs = personCSVDf.as[Person]
  //personCSVDs.show()
  personCSVDs.groupBy($"age").count().show()
  personCSVDs.filter($"age" < 25).show()
  personCSVDs.filter($"name".startsWith("J")).show()

  //  Seq(("Prakash",30)).toDS().show()

  /*
    +----+-------+
    | age|   name|
    +----+-------+
    |null|Michael|
    |  30|   Andy|
    |  19| Justin|
    +----+-------+
	*/
  /* val emp = spark.read.json("data-files//people.json")
  emp.select($"name").show()
  emp.filter($"name".startsWith("J")).show()
  emp.groupBy("age").count().show()
  emp.filter($"age" > 21).show()
  emp.printSchema()
  emp.show()*/

  /*
   * Michael,29
     Andy,30
     Justin,19
   *
   */
  /*val schema = StructType(Array(StructField("name", StringType, true), StructField("age", IntegerType, true)))
  val csvDF = spark.read.schema(schema).csv("data-files//people.txt")

  csvDF.show()
  csvDF.printSchema()
  csvDF.select($"name").show()
  csvDF.filter($"name".startsWith("J")).show()
  csvDF.groupBy("age").count().show()
  csvDF.filter($"age" > 21).show()
  csvDF.printSchema()
  csvDF.show()*/

}