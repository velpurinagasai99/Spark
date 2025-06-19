package DFandSQLfiles

import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Dataframes extends App {
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","spark session application")
  sparkConf.set("spark.master","local[*]")

  val spark =SparkSession.builder()
    .config(sparkConf)                // instead of sparkConf declaration we can directly use
    .getOrCreate()                    // SparkSession.builder
  Configurator.setLevel("org", org.apache.logging.log4j.Level.ERROR)
  val ordersDF = spark.read.csv("C:/Users/velpu/Documents/BigDataTrendyTech/week9/friendsdata-201008-180523.csv")
//  val df = ordersDF.rdd
//  df.foreach(println)
//  scala.io.StdIn.readLine()
  ordersDF.show()
  spark.stop()
}