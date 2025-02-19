package Practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
/*
* Narrow Transformation and Wide Transformation
*
* If we need shuffling(Each row is dependent on other rows) then it is wide transformation.....Eg, ReduceByKey
* If it doesn't require shuffling, then it is Narrow Transformation.....Eg, Filter, map, flatmap
*/
object listToRDD extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "LinkedInConnections")
  val list = List("WARN: Tuesday 4 September 0405",
                  "ERROR: Tuesday 4 September 0408",
                  "ERROR: Tuesday 4 September 0408",
                  "ERROR: Tuesday 4 September 0408",
                  "ERROR: Tuesday 4 September 0408",
                  "ERROR: Tuesday 4 September 0408")
  val logsRDD = sc.parallelize(list)                                //Converting List to RDD
  val processedInput = logsRDD.map(x=>(x.split(":")(0),1))

  /*We can also write in this form....both are same

  val pairs = logsRDD.map(x=>{
  val columns = x.split(":")
  val loglevel = columns(0); (loglevel,1)
  })
   */

  val finalOutput = processedInput.reduceByKey(_+_)
  finalOutput.collect.foreach(println)

  //scala.io.StdIn.readLine()
}
