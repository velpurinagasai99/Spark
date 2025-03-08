package RDDFiles

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object OrderStatus extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "LinkedInConnections")
  val inputData = sc.textFile("C:/Users/velpu/Documents/BigDataTrendyTech/week-12/orders-201025-223502.csv")
  val filteredRdd = inputData.map(x=>(x.split(",")(2),x.split(",")(3)))
  val filteredRdd2 = filteredRdd.mapValues(x=>(x,1)).filter(x=>x._2._1=="CLOSED")
  val filteredRdd3 = filteredRdd2.reduceByKey((x,y)=>(x._1,x._2+y._2))                        //Remember this Nagasai, map accepts (x,y) as parameters of a row.....whereas reducebyKey accepts (x,y) as values of two different key value pairs
  val finalRdd = filteredRdd3.map(x=>(x._1,x._2._2)).filter(x=>x._2>2).sortBy(x=>x._2,false)
  finalRdd.collect.foreach(println)
}
