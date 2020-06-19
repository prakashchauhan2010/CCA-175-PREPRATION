package common

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object ConnectionUtil {
  {
    System.setProperty("hadoop.home.dir", "D://spark-2.2.3-bin-hadoop2.7//winutils");
  }

  var conf = new SparkConf()
    .setAppName("prakash")
    .setMaster("local[5]")

  var sc = SparkContext.getOrCreate(conf)

  var spark = SparkSession.builder()
    .appName("prakash")
    .config("spark.sql.warehouse.dir", "C://Users//prakash.chauhan//workspace//CCA-175//spark-warehouse")
    .getOrCreate()
}