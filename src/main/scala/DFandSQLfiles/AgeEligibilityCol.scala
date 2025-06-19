package DFandSQLfiles

import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

case class customers(name:String,age:Int,city:String)

object Ages extends Serializable {
  def ageCheck(age:Int)={
    if(age>25)
      "Eligible"
    else
      "Not Eligible"
  }
}

object AgeEligibilityCol extends App{

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","spark session application")
  sparkConf.set("spark.master","local[*]")

  val spark =SparkSession.builder()
    .config(sparkConf)                // instead of sparkConf declaration we can directly use
    .getOrCreate()                    // SparkSession.builder
  Configurator.setLevel("org", org.apache.logging.log4j.Level.ERROR)
  val customers = spark.read
    .format("csv")
    .option("inferSchema",true)
    .option("header",true)
    .option("path","C:/Users/velpu/Documents/BigDataTrendyTech/week-12/CustomerDetails.csv")
    .load
  val customersDf:Dataset[Row] = customers.toDF("name","age","city")
  import spark.implicits._

  spark.udf.register("parseagefunction",Ages.ageCheck(_:Int):String)
  val customersFinal = customersDf.withColumn("adult",expr("parseagefunction(age)"))

//  val parseagefunction = udf((age: Int) => Ages.ageCheck(age))
//  val customersFinal = customersDf.withColumn("adult",parseagefunction(col("age")))
  customersFinal.show()
  spark.catalog.listFunctions().filter(x=>x.name == "parseagefunction").show() // doesnt work when we use col object expression
}
