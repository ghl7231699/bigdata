package sparkstreaming

import org.apache.spark.SparkConf

object SparkStreamingApp {
  def main(args: Array[String]): Unit = {
    if (args.length!=3){
      System.err.println("useage:SparkStreaming APP<master> <host> <port>")
    }
    val master=args(0)
    val host=args(1)
    val port=args(2)
    val wordCount = new SparkConf().setAppName("WordCount")
  }

}
