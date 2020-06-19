package rdd

object MapExample {

  import org.apache.spark.SparkContext
  import org.apache.spark.SparkContext._
  import org.apache.spark.SparkConf

    val inputFile = "data-files\\hellospark_ip"

    val conf = new SparkConf().setAppName("Word Count").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ip = sc.textFile(inputFile)

    
    val example1 = ip.map(word => (word, 1)).reduceByKey((a, b) => a + b)

    val example2 = ip.map(word => (word, 1)).groupByKey()
    
    val wordCounts = ip.flatMap(line => line.split(" ")).map(word => (word, 1)).foldByKey(10)((a,b)=>a+b)
    
    //val wordCounts = ip.flatMap(line => line.split(" ")).map(word => (word, 1)).countByKey()
    
    //val op1 = wordCounts.lookup("Vishal")
    // wordCounts.map(x=>(x._1,x._2.toList.sum)).map(x=>x.swap).sortByKey(false).collect().foreach(println)
    //op1.foreach (println)
    //wordCounts.collectAsMap().foreach(println)
    //wordCounts.mapValues(a=>a/2).foreach(println)
    /*
     * Lines below are used to demonstrate
     * the facility of of common number RDD functions 
     */
    // val doubRDD = sc.parallelize(Seq(1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
    //doubRDD.foreach(println)
//    println("mean of elements:-"+ doubRDD.mean+" sum of elements:- "+doubRDD.sum+ " variance of elements:- "+doubRDD.variance)
//    println("total stats:-> "+doubRDD.stats)
}