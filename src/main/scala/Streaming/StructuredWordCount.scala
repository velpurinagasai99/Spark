package Streaming

import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.sql.SparkSession


object StructuredWordCount extends App{

  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("My Streaming Application")
    .config("spark.sql.shuffle.partitions",3)
    .getOrCreate()
  Configurator.setLevel("org", org.apache.logging.log4j.Level.ERROR)

  //read data
  val lines = spark.readStream
    .format("socket")
    .option("host","localhost")
    .option("port","9998")
    .load()
  lines.printSchema()

  //Process data
  val linesdf=lines.selectExpr("explode(split(value,' ')) as word")
  val counts=linesdf.groupBy("word").count()


  //sink Data
  val wordCount = counts.writeStream.format("console")
    .outputMode("complete")
    .option("checkpointLocation","checkpoint-location1")
    .start()

  wordCount.awaitTermination
  println(org.apache.logging.log4j.LogManager.getLogger.getClass)

}
