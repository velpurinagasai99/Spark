package EMRFiles

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}

object OrderStatusEMR {
  def main(args: Array[String]): Unit = {

    val props = ConfigFactory.load()
    val envProps = props.getConfig(args(0))
    val sparkBuilder = SparkSession.builder.appName("Testing EMR")

    if (args(0) == "dev") {
      sparkBuilder.master("local[*]")
    }

    val spark = sparkBuilder.getOrCreate()

    spark.sparkContext.setLogLevel("Error")
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    import spark.implicits._


    val inputBaseDir = envProps.getString("input.base.dir")
    val orders = spark
      .read
      .option("header",true)
      .option("inferSchema",true)
      .csv(inputBaseDir+"Orders.csv")

    orders.createOrReplaceTempView("orders")
    val finalRdd = spark.sql(
      """select order_customer_id, count(*) as total_orders from orders where
        |order_status ='CLOSED' group by order_customer_id having total_orders>2 order by total_orders desc
        |""".stripMargin)

    val outputBaseDir = envProps.getString("output.base.dir")
    finalRdd
      .write
      .format("json")
      .mode(SaveMode.Overwrite)
      .save(outputBaseDir+"orders")
  }
}