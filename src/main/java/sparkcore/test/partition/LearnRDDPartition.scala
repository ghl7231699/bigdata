package sparkcore.test.partition

import org.apache.spark.{SparkConf, SparkContext}

object LearnRDDPartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("learnTextFile")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val textFileRDD = sc.textFile("in/test.csv")
    sc.setLogLevel("ERROR")
    //sc.defaultParallelism 与local的线程数一致
    println(textFileRDD.partitions.length)
    println(textFileRDD.partitioner)

    val rdd1 = sc.parallelize(Seq((1, "jan", 2016), (3, "nov", 2014), (16, "feb", 2014)), 1)
    val rdd2 = sc.parallelize(Seq((5, "dec", 2014), (17, "sep", 2015)), 1)
    val rdd3 = sc.parallelize(Seq((6, "dec", 2011), (16, "may", 2015)), 1)
    val rddUnion = rdd1.union(rdd2).union(rdd3)
    println("rddUnion.partitions.size:" + rddUnion.partitions.length)
    rddUnion.foreach(println)

  }
}
