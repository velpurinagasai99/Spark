package Practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import scala.io.Source

/*
This uses broadcast variable which is similar to map side join in Hive

and also we use filter method(which works only on values in a map) to remove the boaringwords present in broadcast
*/
object RDDCostWithoutBoringWords extends App{
def loadBoaringWords(): Set[String] = {
  var boringWords: Set[String] = Set()
  val lines = Source.fromFile("C:/Users/velpu/Documents/BigDataTrendyTech/week-10/Datasets/boringwords.txt").getLines()
  for (line <- lines)
    boringWords += line
  boringWords
}
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "CostWithoutBoringWords")
  val nameset = sc.broadcast(loadBoaringWords())
  val inputData = sc.textFile("C:/Users/velpu/Documents/BigDataTrendyTech/week-10/Datasets/bigdatacampaigndata.csv")
  val processedInputData = inputData.map(x => (x.split(",")(10).toFloat, x.split(",")(0).toLowerCase()))
  val splittedData = processedInputData.flatMapValues(x=>x.split(" ")).map(x=>(x._2,x._1))
  val filteredData = splittedData.filter(x=> !nameset.value(x._1))                  //Filtering the Data
  val finalOutput = filteredData.reduceByKey((x,y)=>x+y).sortBy(x=>x._2,false)

  finalOutput.collect.foreach(println)
  scala.io.StdIn.readLine()
}