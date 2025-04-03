package DFandSQLfiles

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.date_format
import org.apache.spark.{SparkConf, SparkContext}

object LogsAndPivoting extends App{

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","spark session application")
  sparkConf.set("spark.master","local[*]")

  val spark =SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()


  val mappedLogs : Dataset[Row] = spark.read
    .format("csv")
    .option("header",true)
    .option("inferSchema",true)
    .option("path","C:/Users/rahit/OneDrive/Desktop/BigData/Week-12/biglog.txt")
    .load

  mappedLogs.createOrReplaceTempView("LoggingTable")
  val logsFiltered = spark.sql("select level, date_format(datetime,'MMMM') as month, " +
    " cast(date_format(datetime,'M')as int) as monthnum from LoggingTable")
  logsFiltered.createOrReplaceTempView("loggingTable2")

  val cols = List("January","February","March","April","May","June","July","August","September","October","November","December")

  spark.sql("select level, month" +
    " from LoggingTable2 ").groupBy("level").pivot("month",cols).count().show(6)


//  val groupedLogs = spark.sql("select level, month, monthnum, count(*) as counts" +
//    " from LoggingTable2 group by level,month,monthnum order by monthnum,level ")
//  val finalLogs = groupedLogs.drop("monthnum")
//  finalLogs.show(60)

}
