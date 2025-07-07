package DFandSQLfiles

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object RepartitionandCoalesce extends App {
//Write a code to analyse the difference between Repartition, Coalesce and Repartition after Coalesce
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "CostWithoutBoringWords")
  val inputData = sc.textFile("C:/Users/velpu/Documents/BigDataTrendyTech/week-14/biglog.txt")
  val cs = inputData.getNumPartitions
  val filteredRDD = inputData.filter(line => !line.startsWith("DEBUG"))

  val finalRDD = filteredRDD.repartition(110)           //Just by using Repartition......Analyze DAGS

//  val finalRDD = filteredRDD.coalesce(110)            //Just by using Coalesce

//  val finalrdd1 = filteredRDD.coalesce(110)         //By using Coalesce and repartition
//  val finalRDD = finalrdd1.repartition(110)

  val cs1 = finalRDD.getNumPartitions
  finalRDD.count()
  println(cs,cs1)
  scala.io.StdIn.readLine()
}
