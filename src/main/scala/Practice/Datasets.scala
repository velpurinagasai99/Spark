package Practice

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.sql.Timestamp


case class DS (order_id:Int, order_date: Timestamp,order_customer_id: Int, order_status: String )

object Datasets extends App {
    var sparkConf = new SparkConf()
    sparkConf.set("spark.app.name","spark session application")
    sparkConf.set("spark.master","local[*]")

    val spark =SparkSession.builder()
      .config(sparkConf)                // instead of sparkConf declaration we can directly use
      .getOrCreate()                    // SparkSession.builder

    val ordersDF : Dataset[Row] = spark.read
      .format("csv")
      .option("header",true)
      .option("inferSchema",true)
      .option("path","C:/Users/velpu/Documents/BigDataTrendyTech/week-11/DSTest.csv")
      .load

    import spark.implicits._            //Library to change datatypes
    val ordersDS= ordersDF.as[DS]       //DF to DS
    val ex=ordersDS.filter(x=>x.order_id<1000000)
    val rdd = ordersDF.filter("order_id<10")
    ex.collect.foreach(println)

    spark.stop()

}
