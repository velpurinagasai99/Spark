package Streaming

import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object FileStreaming extends App{
  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("My Streaming Application")
    .config("spark.sql.shuffle.partitions",3)
    .config("spark.sql.streaming.schemaInference","true")
    .getOrCreate()
  Configurator.setLevel("org", org.apache.logging.log4j.Level.ERROR)

  //Read Data
  val orders = spark.readStream
    .format("csv")
    .option("path","C:/Users/velpu/Documents/BigDataTrendyTech/week-16/Orders/")
//    .option("maxFilesPerTrigger",1)                           //To restrict max number of files per Batch
    .option("header","true")
    .load()

  //Process Data
 orders.createOrReplaceTempView("orders1")
  val completedOrders = spark.sql("select * from orders1 where order_status='COMPLETE'")

  //sink Data
  val wordCount = completedOrders.writeStream.format("console")
    .outputMode("append")   //One more type of output mode...complete,update and append
//    .option("path","C:/Users/velpu/Documents/BigDataTrendyTech/week-16/outputs")
    .option("checkpointLocation","checkpoint-location1")
    .trigger(Trigger.ProcessingTime("30 seconds"))
    .start()

  wordCount.awaitTermination
  println(org.apache.logging.log4j.LogManager.getLogger.getClass)
}
