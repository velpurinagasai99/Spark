package Streaming

import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

/*
 we can do left join or right join for streaming Data with satisfying two conditions
1. Right table should be watermarked always
2. Maximum Time constraint between the left and right side streams(i.e water-mark on different time columns)

* */
object StreamingSreamingLeftJoin extends App {
  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("My Streaming Application")
    .config("spark.sql.shuffle.partitions",3)
    .config("spark.sql.streaming.schemaInference","true")
    .getOrCreate()
  Configurator.setLevel("org", org.apache.logging.log4j.Level.ERROR)

  //Schema Definition
  val impressionSchema = StructType(List(
    StructField("impression_id",StringType),
    StructField("impression_time",TimestampType),
    StructField("campaign_type",StringType)
  ))

  val clickSchema = StructType(List(
    StructField("click_id",StringType),
    StructField("click_time",TimestampType)
  ))

  //read data
  val impressions = spark.readStream
    .format("socket")
    .option("host","localhost")
    .option("port","12345")
    .load()

  //read data
  val clicks = spark.readStream
    .format("socket")
    .option("host","localhost")
    .option("port","12346")
    .load()

  //Modify the data
  val impressionDf = impressions.select(from_json(col("value"),impressionSchema)
    .alias("schemaname")).select("schemaname.*")
    .withWatermark("impression_time","30 minute")
  val clicksDf = clicks.select(from_json(col("value"),clickSchema)
    .alias("schemaname")).select("schemaname.*")
    .withWatermark("click_time","30 minute")

  //Join
  val joinCondition = expr("impression_id==click_id AND click_time between impression_time and impression_time + interval 15 minute")
  val joinType = "leftOuter"
  val enrichedData = impressionDf.join(clicksDf,joinCondition,joinType).drop(clicksDf.col("click_id"))

  //write to sink
  val execQuery = enrichedData.writeStream
    .format("console")
    .outputMode("append")
    .option("chcekpointLocation","check-point-location10")
    .trigger(Trigger.ProcessingTime("15 second"))
    .start()

  execQuery.awaitTermination()
}
