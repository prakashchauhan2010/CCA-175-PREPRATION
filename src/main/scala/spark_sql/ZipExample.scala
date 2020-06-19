package spark_sql

import common.ConnectionUtil

object ZipExample extends App{
  val spark = ConnectionUtil.spark
  val sc = ConnectionUtil.sc
  import spark.implicits._

  val file = sc.textFile("data-files/users.csv")
  val fileWOheader = file.filter(_.startsWith("myself")).repartition(1)
  val fileWheader = file.filter(_.startsWith("id")).repartition(1)
  
  fileWOheader.collect().foreach(println)
  println(fileWheader.getNumPartitions)
  fileWheader.collect().foreach(println) 
  println(fileWheader.getNumPartitions)
  
  val x = fileWheader.zip(fileWOheader).collectAsMap()
  x.foreach(println)
}