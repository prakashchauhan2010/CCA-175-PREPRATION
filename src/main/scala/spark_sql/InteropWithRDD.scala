package spark_sql

import common.ConnectionUtil
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import jdk.nashorn.internal.codegen.types.LongType
import jdk.nashorn.internal.codegen.types.LongType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType

/**
 * The Scala interface for Spark SQL supports automatically converting an RDD containing case classes to a DataFrame.
 * The case class defines the schema of the table. The names of the arguments to the case class are read using reflection and become the names of the columns.
 * Case classes can also be nested or contain complex types such as Seqs or Arrays.
 * This RDD can be implicitly converted to a DataFrame and then be registered as a table. Tables can be used in subsequent SQL statements.
 */
object InteropWithRDD extends App {
  val spark = ConnectionUtil.spark
  import spark.implicits._
  val sc = ConnectionUtil.sc
  spark.sparkContext.setLogLevel("ERROR")
  
  
  ///////////////////////////////////////////////////////  Inferring the Schema Using Reflection ////////////////////////////////////
  // Case class can define the schema of the table.
  case class Person(name:String, age: Long)
  
  val data = sc.textFile("data-files//people.txt")
  val df = data.map(line=>line.split(","))
               .map(arr => Person(arr(0),arr(1).trim().toLong))
               .toDF()
  df.show()
  df.printSchema()
  df.createOrReplaceTempView("person")
  spark.sql("select * from person").show()
  val result = spark.sql("select * from person where age > 25")
  spark.sql("select * from person where name like 'M%'").show()
 
  // The columns of a row in the result can be accessed by field index
  // Output:
            /*+-------------+
           		|        value|
              +-------------+
              |Name: Michael|
              |   Name: Andy|
              | Name: Justin|
              +-------------+
          	* 
          	*/
  df.map(row => "Name: "+row(0)).show()
  
  
  // The columns of a row in the result can be accessed by field name.
  // Output will be the same as above.
  df.map(row => "Name: "+row.getAs[String]("name")).show()
  
  
  // Defining explicit encoders for Dataset[Map[K,V]]
  implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String,Any]]
  df.map(row => row.getValuesMap[Any](List("name","age"))).collect().foreach(println)
  
  
  
  /////////////////////////////////////////////////////////////////////////  Programmatically Specifying the Schema ////////////////////////////////////////////////////////////////////
  /**
   * When case classes cannot be defined ahead of time (for example, the structure of records is encoded in a string, or a text dataset will be parsed and fields will be projected differently for different users), a DataFrame can be created programmatically with three steps.

     1. Create an RDD of Rows from the original RDD; i.e from RDD[String] to RDD[Row].
     2. Create the schema represented by a StructType matching the structure of Rows in the RDD created in Step 1.
     3. Apply the schema to the RDD of Rows via createDataFrame method provided by SparkSession.
   *
   */
  
  val peopleRdd = sc.textFile("data-files//people.txt")
  val rowRDD = peopleRdd.map(line => line.split(",")).map(arr => Row(arr(0).trim(),arr(1).trim().toDouble))
  
  val schema = StructType(List(StructField("name",StringType,nullable=true),StructField("age",DoubleType,true)))
  val peopleDF = spark.createDataFrame(rowRDD, schema)
  peopleDF.show()
  peopleDF.printSchema()
  
  
  

}