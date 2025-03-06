package RDDFiles

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/*
After Running check difference by using cache method.
*/
object RDDCostForEachWord extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "LinkedInConnections")
  val inputData = sc.textFile("C:/Users/velpu/Documents/BigDataTrendyTech/week-10/Datasets/bigdatacampaigndata.csv")
  val filteredData = inputData.map(x => (x.split(",")(10).toFloat, x.split(",")(0).toLowerCase()))
  val splittedData = filteredData.flatMapValues(x=>x.split(" ")).map(x=>(x._2,x._1))
  val finalOutput = splittedData.reduceByKey((x,y)=>x+y).sortBy(x=>x._2,false)    //.cache()

  finalOutput.collect.foreach(println)                        //Action1

  val finalOutputDoubled = finalOutput.map(x=>(x._1,x._2*2))
  val finalOutputfiltered = finalOutputDoubled.filter(x=>x._2>100)      //.cache()

  finalOutputfiltered.collect.foreach(println)                //Action2
  println(finalOutput.count())                        //Action3

  scala.io.StdIn.readLine()
}



