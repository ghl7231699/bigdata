package sparkcore

import org.apache.spark.{SparkConf, SparkContext}

object LearnSparkHDFS {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    conf.setAppName("learnTextFile")
    conf.setMaster("local[3]")

    val sc =new SparkContext(conf)

    /**
      * 在向HDFS写入数据时，当前RDD的分区数，就是HDFS上的文件数。
      * 为了避免HDFS上生成大量的小文件，可以对RDD进行reparation，然后再saveAsTextFile。
      */
    //    val textFileRDD=sc.textFile("in/README.md")
//    textFileRDD.map(line => line.toUpperCase()).repartition(5).saveAsTextFile("hdfs://bigdata01:9000/sparkdata5")
    /**
      *从HDFS上读取有5个分区数据，生成的RDD的分区数为5.
      * 注意：HDFS上一个小文件也是一个block。
      */
    val textFileRDD5=sc.textFile("hdfs://bigdata01:9000/sparkdata5")
    println("textFileRDD5 partition size:"+textFileRDD5.partitions.size)
  }
}
