package Streaming
import org.apache.spark.SparkContext
import org.apache.spark.streaming._


object wCS extends App{

  val sc = new SparkContext("local[*]","wordcountStateless")
  sc.setLogLevel("ERROR")
  val ssc = new StreamingContext(sc, Seconds(10))
  val lines = ssc.socketTextStream("localhost",9998)          //in cmd run: ncat -lk 9998
  val words = lines.flatMap(x=>x.split(" ")).map(x=>(x,1))    //open googlechrome and go localhost:9998 and then start typing in
  val wordscount = words.reduceByKey((x,y)=>x+y)              //command Prompt
  wordscount.print()
  ssc.start()
  ssc.awaitTermination()
}


