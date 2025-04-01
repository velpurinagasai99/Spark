package DFandSQLfiles

import DFandSQLfiles.DFExample.spark
import DFandSQLfiles.Datasets.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object regexOrdersStatus extends App {
  val myregex="""^(\S+) (\S+)\t(\S+)\,(\S+)""".r
  case class Orders(order_id:Int,customer_id:Int, order_status:String)
  def parser(line: String)={
    line match {
      case myregex(order_id, date, customer_id, order_status) =>
        Orders(order_id.toInt , customer_id.toInt , order_status )
    }
  }
  var sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "spark session application")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val lines = spark.sparkContext.textFile("C:/Users/user/Desktop/BigData/Orders.txt")
  lines.collect.foreach(println)
  import spark.implicits._
  //val OrdersDS = lines.map(parser)
  //  OrdersDS.select("order_id").show
  //  OrdersDS.groupBy("order_status").count().show
}