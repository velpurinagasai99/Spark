package DFandSQLfiles

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DFExample extends App {
  var sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","spark session application")
  sparkConf.set("spark.master","local[*]")

  val spark =SparkSession.builder()
    .config(sparkConf)                // instead of sparkConf declaration we can directly use
    .getOrCreate()

  val ordersDF = spark.read.csv("orders_new-201019-002101.csv")
  ordersDF.show()
  // scala.io.StdIn.readLine()
  spark.stop()
}
