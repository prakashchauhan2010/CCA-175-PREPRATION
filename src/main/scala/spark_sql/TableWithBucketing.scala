package spark_sql

import common.ConnectionUtil
import org.apache.spark.sql.SaveMode

object TableWithBucketing extends App {
  val spark = ConnectionUtil.spark
  import spark.implicits._
  val sc = ConnectionUtil.sc
  spark.sparkContext.setLogLevel("ERROR")
  
  val people = spark.read.json("data-files/people.json")
  spark.sql("create database  mydb")
  people.write.bucketBy(2, "name").sortBy("age").format("parquet").saveAsTable("mydb.people_bucketed")
  //people.write.format("parquet").saveAsTable("people_bucketed")
}