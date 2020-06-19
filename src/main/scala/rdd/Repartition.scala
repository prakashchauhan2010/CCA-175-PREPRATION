package rdd

import common.ConnectionUtil

object Repartition extends App{
  val spark = ConnectionUtil.spark
  import spark.implicits._
  val sc = ConnectionUtil.sc
  spark.sparkContext.setLogLevel("ERROR")
  
  val inputFile = "data-files\\udf.json"
  var data = spark.read.json(inputFile)
  
  // Original Partitions
  println(data.rdd.getNumPartitions)
  
  // Repartition the data
  var partitionedData = data.repartition(5)
  
  // Final Partitions
  println(partitionedData.rdd.getNumPartitions)
}