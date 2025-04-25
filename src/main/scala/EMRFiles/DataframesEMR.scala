package EMRFiles

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
object DataframesEMR {
  def main(args: Array[String]): Unit ={

//    println("Resolved config:")
//    println(ConfigFactory.load().root().render())
//    println(ConfigFactory.load().root().render())
    val props = ConfigFactory.load()
    val envProps = props.getConfig(args(0))
    val sparkBuilder = SparkSession.builder.appName("Testing EMR")

    if (args(0) == "dev") {
      sparkBuilder.master("local[*]")
    }

    val spark = sparkBuilder.getOrCreate()

    spark.sparkContext.setLogLevel("Error")
    spark.conf.set("spark.sql.shuffle.partitions","2")

    val inputBaseDir = envProps.getString("input.base.dir")
    val order = spark
      .read
      .option("header",true)
      .option("inferschema",true)
      .csv(inputBaseDir+"Orders.csv")
    val outputBaseDir = envProps.getString("output.base.dir")
    order
      .write
      .format("json")
      .mode(SaveMode.Overwrite)
      .save(outputBaseDir+"orders")
  }
}
