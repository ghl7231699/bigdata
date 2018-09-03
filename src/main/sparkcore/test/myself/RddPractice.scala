package test.myself

import org.apache.spark.{SparkConf, SparkContext}

/**
  * RDD相关操作
  */
object RddPractice {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("FlightFilter")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    createRdd(sc)

    createRdd(sc, "in/国内航班数据说明.txt")
    createRdd(sc, "hdfs://ghl01:8020/user/ghl/source/国内航班数据500条.csv")

  }

  /**
    * 使用集合创建RDD
    *
    * 还有一种方式
    * 使用已有RDD创建RDD
    */
  def createRdd(sc: SparkContext): Unit = {
    val data = Array(1, 2, 3, 4, 5, 6)
    val unit = sc.parallelize(data)

    unit.foreach(println)
  }

  /**
    * 从外部数据源创建RDD
    *
    *
    * Spark可以从Hadoop支持的任何存储源创建分布式数据集，包括本地文件系统、HDFS、Cassandra、HBase、Amazon S3等。
    * Spark支持文本文件、SequenceFiles和任何其他Hadoop InputFormat。
    *
    *
    * 1 读取本地文件
    * 2 读取HDFS上的数据
    */
  def createRdd(sc: SparkContext, path: String): Unit = {
    val unit = sc.textFile(path)

    println(unit
      .map(s => s.length)
      .reduce((x, y) => x + y))
  }


}
