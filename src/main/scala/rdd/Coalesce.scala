package rdd

import common.ConnectionUtil

//Differences between coalesce and repartition
//The repartition algorithm does a full shuffle of the data and creates equal sized partitions of data. c
//Coalesce combines existing partitions to avoid a full shuffle.

object Coalesce extends App {
  val spark = ConnectionUtil.spark
  import spark.implicits._
  val sc = ConnectionUtil.sc
  spark.sparkContext.setLogLevel("ERROR")

  val inputFile = "data-files\\udf.json"
  var data = spark.read.json(inputFile).repartition(5)

  // Original Partitions
  println(data.rdd.getNumPartitions)

  // Repartition the data using Coalesce
  var partitionedData = data.coalesce(2)

  // Final Partitions
  println(partitionedData.rdd.getNumPartitions)
}