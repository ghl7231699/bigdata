package sparkcore.partition

import org.apache.hadoop.mapred.lib.HashPartitioner
import org.apache.spark.{SparkConf, SparkContext}

object LearnRDDPartition2 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    conf.setAppName("learnTextFile")
//    conf.set("spark.default.parallelism","3")//设置默认的并行度
    /**
      * local:一个线程 -------- sc.defaultParallelism值为 1
      * local[*]:服务器core数量 ----- sc.defaultParallelism的值为 8
      * local[4]:4个线程 ----- sc.defaultParallelism的值为 4
      *
      * spark.default.parallelism参数值的说明：
      * 如果spark-default.conf或SparkConf中设置了spark.default.parallelism参数值
      * 那么spark.default.parallelism=设置值
      * 如果spark-default.conf或SparkConf中没有设置spark.default.parallelism参数值
      * 那么：
      * local模式：local      spark.default.parallelism = 1
      *            local[4]   spark.default.parallelism = 4
      *
      * yarn和standalone模式：spark.default.parallelism =  max（所有executor使用的core总数， 2）
      *y由上述规则，确定spark.default.parallelism的默认值
      * 当Spark程序执行时，会生成SparkContext对象，同时会生成以下两个参数值：
      *   sc.defaultParallelism     = spark.default.parallelism
      *   sc.defaultMinPartitions = min(spark.default.parallelism,2)
      * 当sc.defaultParallelism和sc.defaultMinPartitions确认了，就可以推算rdd的分区数了。
      *
      * 有三种产生RDD的方式：
      * 1、通过集合创建--val rdd = sc.parallelize(1 to 100)//没有指定分区数，则rdd的分区数=sc.defaultParallelism
      * 2、通过外部存储创建--val rdd = sc.textFile("filePath")
      *    2.1从本地文件生成RDD，没有指定分区数，则默认分区规则为：rdd的分区数 = max(本地file的分片数,sc.defaultMinPartitions)
      *    2.2从hdfs读取数据生成RDD，没有指定分区数，则默认分区规则为：rdd的分区数 = max(hdfs文件的block数,sc.defaultMinPartitions)
      * 3、通过已有RDD产生新的RDD，新RDD的分区数遵循遗传特性。将表格。
      **/
    conf.setMaster("local[4]")
    val sc =new SparkContext(conf)
    sc.setLogLevel("ERROR")
//    println("sc.defaultParallelism: "+sc.defaultParallelism)
//    val rdd = sc.parallelize(1 to 100)
//    println("rdd.partitions.length:"+rdd.partitions.length)
//    println("rdd.partitions.size:"+rdd.partitions.size)

//    val rdd=sc.textFile("in/spark-2.2.0-bin-hadoop2.7.tgz")
//    println("rdd.partitions.length:"+rdd.partitions.length)
//    println("sc.defaultParallelism: "+sc.defaultParallelism)
//    println("sc.defaultMinPartitions: "+sc.defaultMinPartitions)

//    val rdd=sc.textFile("hdfs://bigdata01:9000/sparkdata5")
//    println("rdd.partitions.length:"+rdd.partitions.length)
//    println("sc.defaultParallelism: "+sc.defaultParallelism)
//    println("sc.defaultMinPartitions: "+sc.defaultMinPartitions)
  }
}
