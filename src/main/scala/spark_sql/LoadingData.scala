package spark_sql

import common.ConnectionUtil
import org.apache.spark.sql.SaveMode
import org.apache.spark.util.SizeEstimator

object LoadingData extends App {
  val spark = ConnectionUtil.spark
  import spark.implicits._
  val sc = ConnectionUtil.sc
  spark.sparkContext.setLogLevel("ERROR")
  
  val jsonRDD = sc.makeRDD(Array("{'name':'Yin','address':{'city':'Columbus','state':'Ohio'}}"))
  //spark.read.json(jsonRDD).show()
  
  ////////////// Default data source is Parquet ///////////////////////
  val emp = spark.read.load("data-files//people.parquet")
  println("Dataframe Size:"+SizeEstimator.estimate(emp))
  println(sc.getRDDStorageInfo)
  emp.select($"name",$"age").show()
  //emp.write.mode(SaveMode.Overwrite).save("data-files//people.parquet")
  
  
  ////////////// Manually specifying the data source //////////////////
  /**
   * Data sources are specified by their fully qualified name (i.e., org.apache.spark.sql.parquet), 
   * For built-in sources you can also use their short names (json, parquet, jdbc, orc, libsvm, csv, text).s
   */
//  val emp1 = spark.read.format("json").load("data-files//people.json")
//  emp1.select($"name").write.format("parquet").mode("overwrite").save("data-files//people_names.parquet")
  //spark.read.format("parquet").load("data-files//people_names.parquet").show()
  
  ///////////////// Query the file directly with SQL. ///////////////////// NOT Working.
  //spark.sql("select * FROM parquet.'CCA-175/data-files/people_names.parquet'").show()
  
  
  //////////////// Saving to Persistent Tables ///////////////////////////////////
  //val emp2 = spark.read.format("json").load("data-files//people.json")
  //spark.sql("create database pdb")
  //emp2.select($"name").write.format("parquet").mode("overwrite").saveAsTable("pdb.peopleinfo")
  //spark.sql("show databases").show()
  //spark.sql("use pdb").show()
  //spark.sql("select * from peopleinfo").show()
  //spark.table("peopleinfo").show()
  
  
  
  
  
}