package com.scd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession


object Scd2Run {

  println ("SainIazi")


  
  def main(args: Array[String]): Unit = {
    
    //  val spark=SparkSession.builder().getOrCreate()    
    val sparkSession = SparkSession
      .builder
      .appName("SparkSCD")
      .master("local[2]")
      .getOrCreate()
//To set HADOOP_HOME.
System.setProperty("hadoop.home.dir", "/Users/saifniazi/workspace/software/hadoop-common-2.2.0-bin-master")
// create Spark context with Spark configuration
//val sc = new SparkContext(new SparkConf().setAppName(“Spark WordCount”).setMaster(“local[2]”))


println ("test")
    
  
val base_rdd = sparkSession.sparkContext.textFile("input/snapshot_file.txt")
val delta_rdd = sparkSession.sparkContext.textFile("input/delta_file.txt")


val base_key_rdd = base_rdd.map(x=> x.split(",")).map(x=> (x(0),x))
val delta_key_rdd = delta_rdd.map(x=> x.split(",")).map(x=> (x(0),x))
val only_base_rdd = base_key_rdd.subtractByKey(delta_key_rdd)

val only_base = only_base_rdd.map(x=>(x._2:+"Y").mkString(","))
    
val only_delta_rdd = delta_key_rdd.subtractByKey(base_key_rdd)
val only_delta = only_delta_rdd.map(x=>(x._2:+"Y").mkString(","))
val only_base_delta = only_delta.union(only_base)


val unwanted_rows = base_key_rdd.subtractByKey(delta_key_rdd)
val wanted_rows = base_key_rdd.subtractByKey(unwanted_rows)
val unwanted_rows1 = delta_key_rdd.subtractByKey(base_key_rdd)
val wanted_rows1 = delta_key_rdd.subtractByKey(unwanted_rows1)
val common_records = wanted_rows.union(wanted_rows1)
val joined_rdd = base_key_rdd.join(delta_key_rdd)
val mapped_rdd = joined_rdd.map(x=> {
  if ( (x._2._1)(4) > (x._2._2)(4))
  {
    (x._1,(x._2._1).toList)
  }
  else {
    (x._1,(x._2._2).toList)
  }
})


val reduced = mapped_rdd.reduceByKey {case (x,y) => if ( x(4) > y(4)) x else y }
val reduced_rdd= reduced.map(x=>(x._1,x._2:+"Y"))
val reduced_final = reduced_rdd.map(x=>x._2.mkString(","))
val x = common_records.map(x=>((x._2.head,x._2(4)),x._2.toList))
val y = reduced_rdd.map(x=>((x._2.head,x._2(4)),x._2))
val inactive_records = x.subtractByKey(y)
val inactive_final = inactive_records.map(x=> (x._2 :+"N").mkString(","))
val final_dataset = reduced_final.union(inactive_final).union(only_base_delta)
final_dataset.collect().foreach(println)

  }
  
  
}