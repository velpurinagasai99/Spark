package RDDFiles
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
object RDDCustomerSegrigation extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","customersegrigation")
  val rdd1 = sc.textFile("C:/Users/velpu/Documents/BigDataTrendyTech/week9/customerorders-201008-180523.csv")
  val rdd2 = rdd1.map(x=>(x.split(",")(0),x.split(",")(2).toFloat))
  val rdd3 = rdd2.reduceByKey((x,y)=>x+y).sortBy(x=>x._2)
  rdd3.collect.foreach(println)
  // scala.io.StdIn.readLine()
}