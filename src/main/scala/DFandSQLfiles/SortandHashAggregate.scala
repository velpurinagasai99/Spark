package DFandSQLfiles

import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object SortandHashAggregate extends App{
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","spark session application")
  sparkConf.set("spark.master","local[*]")

  val spark =SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()
  Configurator.setLevel("org", org.apache.logging.log4j.Level.ERROR)
  val ordersDF : Dataset[Row] = spark.read
    .format("csv")
    .option("header",true)
    .option("inferSchema",true)
    .option("path","C:/Users/velpu/Documents/BigDataTrendyTech/week-12/Orders.csv")
    .load

  ordersDF.createOrReplaceTempView("orders")

//  Results Sort Aggregate
  spark.sql("select order_customer_id,date_format(order_date,'MMMM') orderdt,count(1) cnt, first(date_format(" +
    "order_date,'M')) monthnum from orders group by order_customer_id, orderdt").explain           //Sumit sir explained sort aggregate and Hash aggregate is something to do with strings but not with order by.But, it is something to do with order by of strings

//  Results Hash Aggregate
  spark.sql("select order_customer_id,date_format(order_date,'MMMM') orderdt,count(1) cnt, date_format(" +
    "order_date,'M') monthnum from orders group by order_customer_id, orderdt, monthnum").explain           //This leads to Hash Aggregate instead Sort Aggregate as first needs to sort but not group by

//  Results Sort Aggregate
  spark.sql("select order_customer_id,date_format(order_date,'MMMM') orderdt,count(1) cnt, cast(first(date_format(" +
    "order_date,'M')) as int) monthnum from orders group by order_customer_id, orderdt").explain           //This leads to Sort Aggregate even if it is integer type

//  Results Hash Aggregate
  spark.sql("select order_customer_id,date_format(order_date,'MMMM') orderdt,count(1) cnt, first(cast(date_format(" +
    "order_date,'M') as int)) monthnum from orders group by order_customer_id, orderdt").explain

  spark.stop()
}
