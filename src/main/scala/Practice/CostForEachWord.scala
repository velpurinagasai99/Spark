package Practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object CostForEachWord {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","LinkedInConnections")
  val inputData = sc.textFile("C:/Users/velpu/Documents/BigDataTrendyTech/week-10/Datasets/bigdatacampaigndata.csv")
  val filteredData = inputData.map(x=>(x.split(",")(0),x.split(",")(10).toInt))
  filteredData.collect.foreach(println)
}
