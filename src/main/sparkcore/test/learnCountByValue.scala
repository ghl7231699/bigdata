package sparkcore

import org.apache.spark.{SparkConf, SparkContext}

object learnCountByValue {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    conf.setAppName("learnTextFile")
    conf.setMaster("local")

    val sc =new SparkContext(conf)
    val textFileRDD=sc.textFile("in/README.md")
    val flatMapRDD=textFileRDD.flatMap(line =>line.split(" "))
    val countValue=flatMapRDD.countByValue()
    countValue.foreach(t => println(t._1+" : "+t._2))
  }
}
