package Streaming

import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
**************************************   Analyze Dags, Super Interesting   ************************************************
*/

object entireStateFull extends App{

  def updatefunc(newValues:Seq[Int],previousState:Option[Int]):Option[Int]={
    val newCount = previousState.getOrElse(0)+newValues.sum
    Some(newCount)
  }

  val sc = new SparkContext("local[*]","wordcountStateless")
  sc.setLogLevel("ERROR")
  val ssc = new StreamingContext(sc, Seconds(10))
  val lines = ssc.socketTextStream("localhost",9998)          //in cmd run: ncat -lk 9998
  ssc.checkpoint(".")
  val words = lines.flatMap(x=>x.split(" ")).map(x=>(x,1))    //open googlechrome and go localhost:9998 and then start typing in
  val wordscount = words.updateStateByKey(updatefunc)              //command Prompt
  wordscount.print()
  ssc.start()
  ssc.awaitTermination()

}
