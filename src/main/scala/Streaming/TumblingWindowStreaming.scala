package Streaming

import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.sql.functions.{col, from_json, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{SparkSession, functions}

/*
In this we write a Stateful Aggregation of Grouping data for every 15 minutes.
We use watermark becaues we need to set a time limit for our state...or else state becomes full, with no time bounds
In this code we also impose a structure on raw data for socket streamed Data
Usually in output mode we shouldn't use append mode if it is aggregation on streaming Data,
but with watermarking we can use..it works bit differently, state store clean up happens when
time elapses(When no it ensures no further updates), execute and check once.
watermark will not work in completemode..its useless

Testing Data:
{"order_id":57012,"order_date":"2025-02-12 11:05:00","order_customer_id":2765,"order_status":"PROCESSING","amount":200}
{"order_id":45256,"order_date":"2025-02-12 11:12:00","order_customer_id":791,"order_status":"COMPLETE","amount":300}
{"order_id":53974,"order_date":"2025-02-12 11:20:00","order_customer_id":2098,"order_status":"COMPLETE","amount":700}
{"order_id":5335,"order_date":"2025-02-12 11:40:00","order_customer_id":5547,"order_status":"ON_HOLD","amount":500}
{"order_id":29288,"order_date":"2025-02-12 11:25:00","order_customer_id":3943,"order_status":"CLOSED","amount":200}
* */

object TumblingWindowStreaming extends App{
  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("My Streaming Application")
    .config("spark.sql.shuffle.partitions",3)
    .config("spark.sql.streaming.schemaInference","true")
    .getOrCreate()
  Configurator.setLevel("org", org.apache.logging.log4j.Level.ERROR)

  //read data
  val lines = spark.readStream
    .format("socket")
    .option("host","localhost")
    .option("port","9998")
    .load()

  //Modify the data
  val orderSchema = StructType(List(
    StructField("order_id",IntegerType),
    StructField("order_date",TimestampType),
    StructField("order_customer_id",IntegerType),
    StructField("order_status",StringType),
    StructField("amount",IntegerType)
  ))
  val orderDf = lines.select(from_json(col("value"),orderSchema).alias("schemaname")).select("schemaname.*")
  val aggData = orderDf
    .withWatermark("order_date","30 minute")
    .groupBy(window(col("order_date"),"15 minute"))    //3rd Parameter "5 minute" for sliding window instead of tumbling window
    .agg(functions.sum("amount").alias("total_invoice"))

//  aggData.printSchema()
  val formatedAggData = aggData.select("window.*","total_invoice")
  val execQuery = aggData.writeStream
    .format("console")
    .outputMode("update")
    .option("chcekpointLocation","check-point-location1")
    .trigger(Trigger.ProcessingTime("15 second"))
    .start()

  execQuery.awaitTermination()
}
