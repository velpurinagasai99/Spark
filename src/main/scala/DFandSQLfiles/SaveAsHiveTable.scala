package DFandSQLfiles

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object SaveAsHiveTable extends App{
  val sparkConf = new SparkConf()
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
    .option("path","C:/Users/velpu/Documents/BigDataTrendyTech/week-11/orders-201019-002101.csv")
    .load()

  spark.sql("Create database if not exists retail")

  ordersDF.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .partitionBy("order_status")
    .bucketBy(4,"order_customer_id")        //Using Bucket by can be done while saving as a table
    .sortBy("order_customer_id")
    .saveAsTable("retail.orders")

  spark.catalog.listTables("retail").show()
  scala.io.StdIn.readLine()
  spark.stop()
}
