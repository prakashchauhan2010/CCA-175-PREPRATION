package spark_sql

import common.ConnectionUtil
import org.apache.spark.sql.SaveMode

/**
 * Users can start with a simple schema, and gradually add more columns to the schema as needed.
 * In this way, users may end up with multiple Parquet files with different but mutually compatible schemas.
 * The Parquet data source is now able to automatically detect this case and merge schemas of all these files.
 *
 * You may enable it by:
 * setting data source option mergeSchema to true when reading Parquet files (as shown in the examples below), or
 * setting the global SQL option spark.sql.parquet.mergeSchema to true.
 */
object SchemaEvolution extends App {
  val spark = ConnectionUtil.spark
  import spark.implicits._
  val sc = ConnectionUtil.sc
  spark.sparkContext.setLogLevel("ERROR")
  
  val squaresDF = sc.makeRDD(1 to 5).map(i => (i, i * i)).toDF("value", "square")
  squaresDF.write.format("parquet").mode(SaveMode.Overwrite).save("data-files//schema_test_table//key=1")
  
  val cubesDF = sc.makeRDD(6 to 10).map(i => (i, i * i * i)).toDF("value", "cube")
  cubesDF.write.format("parquet").mode(SaveMode.Overwrite).save("data-files//schema_test_table//key=2")
  cubesDF.show()
  
  val mergedSchema = spark.read.option("mergeSchema", "true").format("parquet").load("data-files//schema_test_table")
  mergedSchema.sort("value").show()
  mergedSchema.printSchema()

}