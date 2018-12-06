package sparkcore.test.myself

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * RDD partition 使用
  */
object RddPartitionPractice {
  var sc: SparkContext = null

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName(this.getClass.getSimpleName)
    conf.setMaster("local")
    //    conf.setMaster("local[4]")
    //    conf.setMaster("local[*]")

    sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val data = sc.textFile("in/test.csv")

    RddPartitions(sc, data)


  }

  /**
    * RDD 属性
    *
    * @param sc
    * @param data
    */
  private def RddPartitions(sc: SparkContext, data: RDD[String]): Unit = {
    println("分区数量为\t" + data.partitions.length)
    println("分区器的类型为\t" + data.partitioner)

    println("线程数\t" + sc.defaultParallelism)

    data.partitions.foreach(x => println(x))

    val rdd1 = sc.parallelize(Seq((1, "jan", 2016), (3, "nov", 2014), (16, "feb", 2014)), 1)
    val rdd2 = sc.parallelize(Seq((5, "dec", 2014), (17, "sep", 2015)), 1)
    val rdd3 = sc.parallelize(Seq((6, "dec", 2011), (16, "may", 2015)), 1)
    val rddUnion = rdd1.union(rdd2).union(rdd3)
    println("rddUnion.partitions.size:" + rddUnion.partitions.length)
    rddUnion.foreach(println)
  }

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
    * local[4]   spark.default.parallelism = 4
    *
    * yarn和standalone模式：spark.default.parallelism =  max（所有executor使用的core总数， 2）
    * y由上述规则，确定spark.default.parallelism的默认值
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

  /**
    * API调用	                            partition.size 	                                partitioner
    *
    *
    *
    * map(),flatMap(),distinct()	        与父RDD相同	                                        NONE
    * filter()	                          与父RDD相同	                                       与父RDD相同
    *rdd.union(otherRDD)	              rdd.partitions.size + otherRDD. partitions.size	    NONE
    *rdd.intersection(otherRDD)	    max(rdd.partitions.size, otherRDD. partitions.size)	    NONE
    *rdd.subtract(otherRDD)	              rdd.partitions.size                             	NONE
    *rdd.cartesian(otherRDD)	      rdd.partitions.size * otherRDD. partitions.size	        NONE
    *
    * reduceByKey(),foldByKey(),          与父RDD相同                                     HashPartitioner
    * combineByKey(),groupByKey()
    *
    * sortByKey()	                        与父RDD相同	                                    RangePartitioner
    *
    * mapValues(),flatMapValues()	        与父RDD相同	                                    父RDD的 partitioner
    * cogroup(),join(),leftOuterJoin(),
    *
    * rightOuterJoin()           	        取决于所涉及的两个RDDs的某些输入属性                  HashPartitioner
    *
    */
}
