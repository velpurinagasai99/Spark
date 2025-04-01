package DFandSQLfiles
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
object DataframesEMR {
  def main(args: Array[String]): Unit ={
    val props = ConfigFactory.load()
    val envProps = props.getConfig(args(0))
    val spark = SparkSession
      .builder
      .appName("Testing EMR")
      .master(envProps.getString("execution.mode"))
      .getOrCreate

    spark.sparkContext.setLogLevel("Error")
    spark.conf.set("spark.sql.shuffle.partitions","2")

    import spark.implicits._

    val inputBaseDir = envProps.getString("input.base.dir")
    val order = spark
      .read
      .option("header",true)
      .option("inferschema",true)
      .csv(inputBaseDir+"/Orders")
    val outputBaseDir = envProps.getString("output.base.dir")
    order
      .show()
  }
}
