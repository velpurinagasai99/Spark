package DFandSQLfiles

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DFExample extends App {
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","spark session application")
  sparkConf.set("spark.master","local[*]")

  val spark =SparkSession.builder()
    .config(sparkConf)                // instead of sparkConf declaration we can directly use
    .getOrCreate()

  val ordersDF = spark.read.option("header",true).csv("C:/Users/velpu/Documents/BigDataTrendyTech/week-11/orders-201019-002101.csv")
  val groupedOrders = ordersDF.repartition(4).where("order_customer_id>10000")
    .select("order_id","order_customer_id")
    .groupBy("order_customer_id")
    .count()
  groupedOrders.show()
  scala.io.StdIn.readLine()     //Analyse DAG for a spark sql command
  spark.stop()
}
