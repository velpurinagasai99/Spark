package DFandSQLfiles

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ColumnObjects extends App {
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","spark session application")
  sparkConf.set("spark.master","local[*]")

  val spark =SparkSession.builder()
    .enableHiveSupport()
    .config(sparkConf)
    .getOrCreate()

  val ordersDF = spark.read
    .format("csv")
    .option("header",true)
    .option("inferschema",true)
    .option("path","C:/Users/rahit/OneDrive/Desktop/BigData/Week-12/orders-201025-223502.csv")
    .load()
  import spark.implicits._

  val col_string = ordersDF.select("order_id")                    //Accessing using column objects
  val col_obj = ordersDF.select(col("order_id"),column("order_id"),$"order_id",'order_id) //Accessing using column objects

  ///can not concat directly, it needs to be wrapped in a expr and then execute it.
  ordersDF.select(col("order_id"),expr("concat(order_status,' STATUS')")).show()     //Accessing using column expression

}
