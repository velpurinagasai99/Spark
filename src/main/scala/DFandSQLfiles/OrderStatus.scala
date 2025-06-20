package DFandSQLfiles

import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object OrderStatus extends App{
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","spark session application")
  sparkConf.set("spark.master","local[*]")

  val spark =SparkSession.builder()
    .config(sparkConf)                // instead of sparkConf declaration we can directly use
    .getOrCreate()                    // SparkSession.builder
  Configurator.setLevel("org", org.apache.logging.log4j.Level.ERROR)
  val ordersDF : Dataset[Row] = spark.read
    .format("csv")
    .option("header",true)
    .option("inferSchema",true)
    .option("path","C:/Users/velpu/Documents/BigDataTrendyTech/week-12/Orders.csv")
    .load

  ordersDF.createOrReplaceTempView("orders")

  val finalRdd = spark.sql(
    """select order_customer_id, count(*) as total_orders from orders where
      |order_status ='CLOSED' group by order_customer_id having total_orders>2 order by total_orders desc
      |""".stripMargin)

  finalRdd.show()

  scala.io.StdIn.readLine()
  spark.stop()

}
