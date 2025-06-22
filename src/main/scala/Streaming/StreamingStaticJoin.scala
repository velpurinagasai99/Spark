package Streaming

import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
/*
We can have only Inner Join, Stream side join....which means left join possible when left table is streaming
right side join possible when streaming is right side.
Full is never possible
* */
object StreamingStaticJoin extends App{
  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("My Streaming Application")
    .config("spark.sql.shuffle.partitions",3)
    .config("spark.sql.streaming.schemaInference","true")
    .getOrCreate()
  Configurator.setLevel("org", org.apache.logging.log4j.Level.ERROR)

  //Schema Definition
  val transactionSchema = StructType(List(
    StructField("card_id",LongType),
    StructField("amount",IntegerType),
    StructField("post_code",IntegerType),
    StructField("pos_id",LongType),
    StructField("transaction_dt",TimestampType)
  ))
  //read data
  val lines = spark.readStream
    .format("socket")
    .option("host","localhost")
    .option("port","9998")
    .load()

  //Modify the data
  val transactionDf = lines.select(from_json(col("value"),transactionSchema).alias("schemaname")).select("schemaname.*")
//    .withWatermark("transaction_dt","30 minute")

  //Load static Dataframe
  val memberDf = spark.read
    .format("csv")
    .option("header",true)
    .option("inferSchema",true)
    .option("path","C:/Users/velpu/Documents/BigDataTrendyTech/week-16/member_id.csv")
    .load()

  //Join
  val joinCondition = transactionDf.col("card_id") === memberDf.col("card_id")
  val joinType = "inner"
  val enrichedData = transactionDf.join(memberDf,joinCondition,joinType).drop(memberDf.col("card_id"))

  //write to sink
  val execQuery = enrichedData.writeStream
    .format("console")
    .outputMode("update")
    .option("chcekpointLocation","check-point-location1")
    .trigger(Trigger.ProcessingTime("15 second"))
    .start()

  execQuery.awaitTermination()

}
