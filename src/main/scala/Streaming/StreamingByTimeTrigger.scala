package Streaming

import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

/*
In streaming we have 4 types first 2 are more imp.
1. Unspecified(Where ever there is update in data batch is triggered) like StructuredWordCount program
2. Time specified(for every specific time interval and new data entry) i.e, this program. For proper understanding Analyze DAGS
3. One time(Happens on longer durations of time interval)
4. Continous Every milli/microsecond it occurs
 */
object StreamingByTimeTrigger extends App{
  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("My Streaming Application")
    .config("spark.sql.shuffle.partitions",3)
    .getOrCreate()
  Configurator.setLevel("org", org.apache.logging.log4j.Level.ERROR)
  val lines = spark.readStream
    .format("socket")
    .option("host","localhost")
    .option("port","9998")
    .load()
  lines.printSchema()

  val linesdf=lines.selectExpr("explode(split(value,' ')) as word")
  val counts=linesdf.groupBy("word").count()

  //???????????????????????????????????Why is trigger condition below Group by clause. It should be above right? Isn't it first divide and then group
  val wordCount = counts.writeStream.format("console")
    .outputMode("update")   //One more type of output mode...complete,update and append
    .option("checkpointLocation","checkpoint-location1")
    .trigger(Trigger.ProcessingTime("30 seconds"))
    .start()

  wordCount.awaitTermination
  println(org.apache.logging.log4j.LogManager.getLogger.getClass)
}
