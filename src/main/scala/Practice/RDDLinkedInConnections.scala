package Practice
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
object RDDLinkedInConnections extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","LinkedInConnections")
  val inputData = sc.textFile("C:/Users/velpu/Documents/BigDataTrendyTech/week9/friendsdata-201008-180523.csv")
  val filteredData = inputData.map(x=>(x.split("::")(2).toInt,x.split("::")(3).toInt))
  val mappedData = filteredData.map(x=>(x._1,(x._2,1)))                       //filteredData.mapValues(x=>(x,1))
  val classifiedData = mappedData.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
  val averageData = classifiedData.map(x=>(x._1,x._2._1/x._2._2)).sortByKey() //classifiedData.mapValues(x=>x._1/x._2)
  averageData.collect.foreach(println)
  // scala.io.StdIn.readLine()
}



