package spark_sql

import common.{ ConnectionUtil }
object RDDExamples extends App {
  val spark = ConnectionUtil.spark
  val sc = ConnectionUtil.sc
  import spark.implicits._

  val file = sc.textFile("data-files/users.csv")
  val fileWOheader = file.filter(!_.startsWith("id")).filter(!_.startsWith("myself"))
  val finalData = fileWOheader.map(line => {
    val words = line.split(",")
    (words(0), words(1))
  })
  finalData.collectAsMap().foreach(println)

  val csv = sc.textFile("data-files/users.csv")
  val headerAndRows = csv.map(line => line.split(",").map(_.trim))
  val header = headerAndRows.first // Array[String]
  val data = headerAndRows.filter(_(0) != header(0))

  /*
   *  Zips one RDD with another one, returning key-value pairs.
   *  There is a condition when using zip function that the two RDDs should have the same number of partitions
   *  and the same number of elements in each partition so something like one rdd was made through a map on the other rdd.
   */
  val maps = data.map(splits => header.zip(splits).toMap)
  val result = maps.filter(x => x("id") != "myself")
  result.collect.foreach(println)

}