package RDDFiles

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


object AgeEligibilityCol extends App{
  def agecheck(name:String,age:Int,City:String)={
    if(age>25) (name,age,City,"eligible")
    else (name,age,City,"not eligible")
  }
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "LinkedInConnections")
  val inputData = sc.textFile("C:/Users/rahit/OneDrive/Desktop/BigData/Week-12/CustomerDetails.txt")
  val mappeddata = inputData.map(x=>(x.split(',')(0),(x.split(',')(1).toInt,x.split(',')(2))))
  val addedcol = mappeddata.map(x=>if(x._2._1>25) (x._1,x._2._1,x._2._2,"eligible")
  else (x._1,x._2._1,x._2._2,"not eligible"))
  val finalrdd = addedcol.map(x=>(x._1,(x._2,(x._3,x._4))))
  finalrdd.collect.foreach(println)
}