package RDDFiles

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import scala.io.Source

//******Use versions related to spark and scala-1.0 when running this file*********

object RDDTopMovies extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "TopMovies")
  val inputData = sc.textFile("C:/Users/velpu/Documents/BigDataTrendyTech/week-11/ratings-201019-002101.dat")
  val idRatings = inputData.map(x=>(x.split("::")(1),x.split("::")(2))).mapValues(x=>(x.toFloat,1.0))
  val idRatingsCountinFloat = idRatings.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
  val moviesWithMoreRatings = idRatingsCountinFloat.filter(x=>x._2._2>100)
  val avgMovieRating = moviesWithMoreRatings.map(x=>(x._1,(x._2._1/x._2._2).toFloat))
  val topMovies = avgMovieRating.filter(x=>(x._2>4.3))
//  val bcvar = sc.broadcast(topMovies.collect.toMap)
  val filepath = Source.fromFile("C:/Users/velpu/Documents/BigDataTrendyTech/week-11/movies-201019-002101.dat","ISO-8859-1")
  val movieMap = filepath.getLines().map { line =>
      val parts = line.split("::")
      parts(0) -> parts(1)  // Convert movieId to Int and take title
    }.toMap

  // Broadcast the map
  val broadcastMovieMap: Broadcast[Map[String, String]] = sc.broadcast(movieMap)
  val joinedMap = topMovies.flatMap { case(key, value) =>
    broadcastMovieMap.value.get(key).map { otherValue =>
      (key, (value, otherValue))
    }
  }
  val topMoviesNames=joinedMap.map(x=>x._2._2)
  joinedMap.collect.foreach(println)
  scala.io.StdIn.readLine()
}

// Without broadcasting, for DAG analysis purpose.
/*
object TopMovies extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "TopMovies")
  val inputData = sc.textFile("C:/Users/velpu/Documents/BigDataTrendyTech/week-11/ratings-201019-002101.dat")
  val idRatings = inputData.map(x=>(x.split("::")(1),x.split("::")(2))).mapValues(x=>(x.toFloat,1.0))
  val idRatingsCountinFloat = idRatings.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
  val moviesWithMoreRatings = idRatingsCountinFloat.filter(x=>x._2._2>100)
  val avgMovieRating = moviesWithMoreRatings.map(x=>(x._1,(x._2._1/x._2._2).toFloat))
  val topMovies = avgMovieRating.filter(x=>(x._2>4.3))
  val moviesData = sc.textFile("C:/Users/velpu/Documents/BigDataTrendyTech/week-11/movies-201019-002101.dat")
  val splittedMoviesData = moviesData.map(x=>(x.split("::")(0),x.split("::")(2)))

  val joinedMap = splittedMoviesData.join(topMovies)
  val topMoviesNames=joinedMap.map(x=>x._2._1)
  joinedMap.collect.foreach(println)
  scala.io.StdIn.readLine()
}
 */
