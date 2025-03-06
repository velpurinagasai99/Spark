package Practice
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
object RDDRatingsClassification extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","ratingsclassification")
  val rdd1 = sc.textFile("C:/Users/velpu/Documents/BigDataTrendyTech/week9/moviedata-201008-180523.txt.data")
  val rdd2 = rdd1.map(x=>(x.split("\t")(2),1))
  val rdd3 = rdd2.reduceByKey((x,y)=>x+y).sortBy(x=>x._2)
  rdd3.collect.foreach(println)
  // scala.io.StdIn.readLine()
}