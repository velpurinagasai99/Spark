package DFandSQLfiles

import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object AggregationFunctions extends App{

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","spark session application")
  sparkConf.set("spark.master","local[*]")

  val spark =SparkSession.builder()
    .config(sparkConf)                // instead of sparkConf declaration we can directly use
    .getOrCreate()                    // SparkSession.builder
  Configurator.setLevel("org", org.apache.logging.log4j.Level.ERROR)
  val customers : Dataset[Row] = spark.read
    .format("csv")
    .option("header",true)
    .option("inferSchema",true)
    .option("path","C:/Users/rahit/OneDrive/Desktop/BigData/Week-12/order_data-201025-223502.csv")
    .load

  customers.createOrReplaceTempView("customersTable")
  customers.show()
  ////Simple Aggregation
  customers.select(count("*").as("total_counts")).show()
  customers.select(expr("count(*)").as( "total_counts")).show()
  spark.sql("select count(*) as total_counts from customersTable").show()


  ////GroupBy Aggregations
  customers.groupBy(col("CustomerID"))
    .agg(sum(expr("Quantity * UnitPrice")) as("Spendings"))
    .show()
  customers.groupBy(col("CustomerID"))
    .agg(expr("sum(Quantity * UnitPrice) as Spendings"))
    .show()
  spark.sql("select CustomerID,sum(Quantity*UnitPrice) as Spendings from customersTable group by CustomerID").show()


  ////WindowFunctions
  val windowstr = Window.partitionBy(col("CustomerID"))
    .orderBy("InvoiceDate")
    .rowsBetween(Window.unboundedPreceding,Window.currentRow)
  val aggregatedCustomers= customers.withColumn("RunningTotal",sum(expr("Quantity*UnitPrice")).over(windowstr))
  aggregatedCustomers.show()

  val aggCustomersRenamed = aggregatedCustomers.withColumnRenamed("InvoiceDate","InvDate")
  aggCustomersRenamed.show()

}
