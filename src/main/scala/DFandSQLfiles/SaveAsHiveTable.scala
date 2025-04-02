package DFandSQLfiles

import DFandSQLfiles.SinkData.{ordersDF, spark}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object SaveAsHiveTable extends App{
  var sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","spark session application")
  sparkConf.set("spark.master","local[*]")

  val spark =SparkSession.builder()
    .enableHiveSupport()                  //Add Hive Jar and enable hive support
    .config(sparkConf)
    .getOrCreate()

  val ordersDF = spark.read
    .format("csv")
    .option("header",true)
    .option("inferschema",true)
    .option("path","C:/Users/rahit/OneDrive/Desktop/BigData/Week-12/orders-201025-223502.csv")
    .load()

  spark.sql("Create database if not exists retail")

  ordersDF.write
    .format("csv")
    .mode(SaveMode.Overwrite)           //Save As hive Table under retail database
    .saveAsTable("retail.orders")

  spark.catalog.listTables("retail").show()
  spark.stop()
}
