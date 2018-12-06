package sparkcore

import org.apache.spark.{SparkConf, SparkContext}

object learnFilter {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    conf.setAppName("learnTextFile")
    conf.setMaster("local")

    val sc =new SparkContext(conf)
    val textFileRDD=sc.textFile("in/README.md")
    //过滤出包含单词"spark"的行
    val filterRDD=textFileRDD.filter(line =>line.contains("spark"))
    println("count:"+filterRDD.count())
  }
}
