package rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object FoldByKey extends App {
  {
    System.setProperty("hadoop.home.dir", "D://spark-2.2.3-bin-hadoop2.7//winutils")
  }
  /**
   *  foldByKey [Pair]
   *  Very similar to fold, but performs the folding separately for each key of the RDD.
   *  This function is only available if the RDD consists of two-component tuples.
   *  foldByKey on the other hand should be used in use cases where values need to be aggregated based on keys.
   *
   *  Listing Variants
   *  def foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)]
   *  def foldByKey(zeroValue: V, numPartitions: Int)(func: (V, V) => V): RDD[(K, V)]
   *  def foldByKey(zeroValue: V, partitioner: Partitioner)(func: (V, V) => V): RDD[(K, V)]
   */
  val inputFile = "data-files\\fold_by_key_ip_1"

  val conf = new SparkConf().setAppName("Word Count").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val ip = sc.textFile(inputFile)

  // Example 1 : Word Count Program
  val example1 = ip.flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .foldByKey(0)((a, b) => a + b)
    example1.collect().foreach(println)
}