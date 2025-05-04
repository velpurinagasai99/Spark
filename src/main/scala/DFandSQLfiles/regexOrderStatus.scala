package DFandSQLfiles

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
case class Orders(order_id:Int,customer_id:Int, order_status:String)

object Helper extends Serializable {

  }


object regexOrdersStatus extends App {

  val myregex="""^(\S+) (\S+)\t(\S+)\,(\S+)""".r
  def parser(line: String)={
    line match {
      case myregex(order_id, date, customer_id, order_status) =>
        Orders(order_id.toInt , customer_id.toInt , order_status )
    }
  }
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "spark session application")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val lines = spark.sparkContext.textFile("C:/Users/velpu/Documents/BigDataTrendyTech/week-11/Orders.txt")

  import spark.implicits._
  val OrdersDS = lines.map(x=>parser(x)).toDF()
  OrdersDS.show()
  //  OrdersDS.groupBy("order_status").count().show
}