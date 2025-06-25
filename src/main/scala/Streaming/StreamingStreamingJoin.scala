package Streaming

import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

/*
Both sides streaming Joins. To avoid state store memory issues we use watermark for cleanup
 */
object StreamingStreamingJoin extends App{

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
  val impressionDf = impressions.select(from_json(col("value"),impressionSchema).alias("schemaname")).select("schemaname.*")
  val clicksDf = clicks.select(from_json(col("value"),clickSchema).alias("schemaname")).select("schemaname.*")

  //Join
  val joinCondition = impressionDf.col("impression_id") === clicksDf.col("click_id")
  val joinType = "inner"
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
