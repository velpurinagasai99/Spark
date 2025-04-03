package DFandSQLfiles

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession,DataFrame}

object ListToDf extends App{
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","spark session application")
  sparkConf.set("spark.master","local[*]")

  val spark =SparkSession.builder()
    .config(sparkConf)                // instead of sparkConf declaration we can directly use
    .getOrCreate()

  val myList = (( 1,"25-07-2013",11599,"CLOSED"),
    (2,"25-07-2013",256,"PENDING_PAYMENT"),
    (3,"25-07-2013",12111,"COMPLETE"),
    (4,"25-07-2013",8827,"CLOSED"),
    (5,"25-07-2013",11318,"COMPLETE")
  )

  import spark.implicits._

}
