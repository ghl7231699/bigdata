package sparkcore.test.myself

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import utils.HdfsUtils

/**
  * 分区器的使用
  */
object PartitionPractice {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("PartitionPractice").setMaster("local[4]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val nums = (0 to 9).toList
    val NumPartitions = sc.parallelize(nums)

    println("NumPartitions:" + NumPartitions.partitions.size)
    println("Partitioner:" + NumPartitions.partitioner)

    val bool = HdfsUtils.deleteFile(new Configuration(), "out")
    if (!bool) {
      //在前面4个分区的情况下，使用分区器分为两个分区，并输出。不同分区器的输出内容
      val pair = NumPartitions.map(x => (x, x))
      pair.saveAsTextFile("out/hashPartition4")

      pair.partitionBy(new HashPartitioner(2))
        .saveAsTextFile("out/hashPartition2")

      //合并分区
      pair.partitionBy(new HashPartitioner(2))
        .saveAsTextFile("out/hashPartition42")

      //合并分区 coalesce方法只能用来减少分区数量，不能用来增加分区数量
      //repartition方法可以用来减少分区数量，也可以用来增加分区数量
      pair.coalesce(2).saveAsTextFile("out/hashPartition22")
    }

  }

}
