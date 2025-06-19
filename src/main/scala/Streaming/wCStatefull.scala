package Streaming

import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
**************************************   Analyze Dags, Super Interesting   ************************************************
*
* reduceByWindow is worked on single value where as reduceByKeyAndWindow works on Key, Value Pairs, remaining everything is same
* countByWindow counts the number of lines we have typed(accepts only two parameters window, slide size)...that's it
*/

object wCStatefull extends App{

  def updatefunc(newValues:Seq[Int],previousState:Option[Int]):Option[Int]={
    val newCount = previousState.getOrElse(0)+newValues.sum
    Some(newCount)
  }

  val sc = new SparkContext("local[*]","wordcountStateless")
  sc.setLogLevel("ERROR")
  val ssc = new StreamingContext(sc, Seconds(10))
  Configurator.setLevel("org", org.apache.logging.log4j.Level.ERROR)
  val lines = ssc.socketTextStream("localhost",9998)          //in cmd run: ncat -lk 9998
  ssc.checkpoint(".")
  val words = lines.flatMap(x=>x.split(" ")).map(x=>(x,1))    //open googlechrome and go localhost:9998 and then start typing in command Prompt

  val wordscount = words.updateStateByKey(updatefunc)    //words.reduceByKeyAndWindow((x,y)=>x+y,(x,y)=>x-y,Seconds(30),Seconds(20)) //Window streaming
  wordscount.print()
  ssc.start()
  ssc.awaitTermination()

}
