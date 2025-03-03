package Practice

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Dataframes extends App {
  var sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","spark session application")
  sparkConf.set("spark.master","local[2]")

  val spark =SparkSession.builder()
    .config(sparkConf)                // instead of sparkConf declaration we can directly use
    .getOrCreate()                    // SparkSession.builder

  val ordersDF: Dataset[Row] = spark.read.csv("C:/Users/velpu/Documents/BigDataTrendyTech/week9/customerorders-201008-180523.csv")
  ordersDF.show()

  scala.io.StdIn.readLine()
  spark.stop()
}


