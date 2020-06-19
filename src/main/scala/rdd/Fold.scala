package rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Fold extends App{
  val inputFile = "data-files\\hellospark_ip"

  val conf = new SparkConf().setAppName("Word Count").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val ip = sc.textFile(inputFile)

  /**
   * Fold
   * Aggregates the values of each partition.
   * The aggregation variable within each partition is initialized with zeroValue.
   *
   * Signature:
   * def fold(zeroValue: T)(op: (T, T) => T): T
   */

  // Example 1
  val a = sc.parallelize(List(1, 2, 3), 3)
  a.fold(0)(_ + _)

  // Example 2
  val b = sc.parallelize(List(1, 2, 3), 3)
  b.fold(1)(_ + _)

  // Example 3 - Find the Item with maximum price.
  val itemPrice = List(("Soap", 10.0), ("Toaster", 200.0), ("Tshirt", 400.0))
  val itemRDD = sc.parallelize(itemPrice)

  // This is zeroValue
  val dummyItem = ("dummy", 0.0)

  val maxPrice = itemRDD.fold(dummyItem)((acc, item) => { if (acc._2 < item._2) item else acc })
  println("maximum price item " + maxPrice)
}