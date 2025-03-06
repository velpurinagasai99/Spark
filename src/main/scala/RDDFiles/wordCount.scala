package RDDFiles
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
object wordCount extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","wordcount")
  val rdd1 = sc.textFile("C:/Users/velpu/Documents/BigDataTrendyTech/week9/search_data.txt")
  val rdd2 = rdd1.flatMap(x=>x.split(" "))
  val rdd3 = rdd2.map(x=>(x,1))
  val rdd4 = rdd3.reduceByKey((x,y)=>x+y).map(x=>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1))
  rdd4.collect.foreach(println)
 // scala.io.StdIn.readLine()                               //To read input(To pause to check localhost:4040)
}
