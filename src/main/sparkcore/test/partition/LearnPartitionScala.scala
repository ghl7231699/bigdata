package sparkcore.partition

import org.apache.spark.{HashPartitioner, RangePartitioner, SparkConf, SparkContext}
object LearnPartitionScala {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    conf.setMaster("local[4]")
    conf.setAppName("test")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val nums=(0 to 9).toList
    val rdd=sc.parallelize(nums)
    println("NumPartitions:"+rdd.getNumPartitions)
    println("Partitioner:"+rdd.partitioner)
    //在前面4个分区的情况下，使用分区器分为两个分区，并输出。不同分区器的输出内容
    val pairRDDp4=rdd.map(num =>(num,num))
    pairRDDp4.saveAsTextFile("out/hashPartition4")
    pairRDDp4.partitionBy(new HashPartitioner(4)).saveAsTextFile("out/hashPartition42")
    //合并分区
    pairRDDp4.partitionBy(new HashPartitioner(2)).saveAsTextFile("out/hashPartition2")
    //合并分区 coalesce方法只能用来减少分区数量，不能用来增加分区数量
    //repartition方法可以用来减少分区数量，也可以用来增加分区数量
    pairRDDp4.coalesce(2).saveAsTextFile("out/hashPartition22")
  }
}
