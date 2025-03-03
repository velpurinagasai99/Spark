package Practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object TopMovies extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "TopMovies")
  val inputData = sc.textFile("C:/Users/velpu/Documents/BigDataTrendyTech/week-11/ratings-201019-002101.dat")
  val idRatings = inputData.map(x=>(x.split("::")(1),x.split("::")(2))).mapValues(x=>(x.toFloat,1.0))
  val idRatingsCountinFloat = idRatings.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
  val moviesWithMoreRatings = idRatingsCountinFloat.filter(x=>x._2._2>100)
  val avgMovieRating = moviesWithMoreRatings.map(x=>(x._1,(x._2._1/x._2._2).toFloat))
  val topMovies = avgMovieRating.filter(x=>(x._2>4.3))
  var varTopMovies = sc.broadcast(topMovies)
  
}
