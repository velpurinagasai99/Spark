package DFandSQLfiles

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object SinkData extends App{
  var sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","spark session application")
  sparkConf.set("spark.master","local[*]")

  val spark =SparkSession.builder()
    .config(sparkConf)                // instead of sparkConf declaration we can directly use
    .getOrCreate()

  val ordersDF = spark.read
    .format("csv")
    .option("header",true)
    .option("inferschema",true)
    .option("path","C:/Users/rahit/OneDrive/Desktop/BigData/Week-12/orders-201025-223502.csv")
    .load()
  ordersDF.write
    .format("json")
    .partitionBy("order_status")
    .mode(SaveMode.Overwrite)
    .option("path","C:/Users/rahit/OneDrive/Desktop/BigData/Week-12/outputs")
    .save()
  spark.stop()

}
