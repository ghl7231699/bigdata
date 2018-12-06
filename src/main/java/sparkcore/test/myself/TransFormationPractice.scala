package sparkcore.test.myself

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * transformation 操作
  */
object TransFormationPractice {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("TransFormationPractice")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    //    val rDd = sc.textFile("in/国内航班数据说明.txt")
    val rDd = sc.textFile("in/test.csv")
    //    map(rDd)
    //    flatMap(rDd)
    //    filter(rDd)
    //
    //    distinct(rDd)
    //
    //    mapPartitions(rDd)
    //
        mapPartitionWithIndex(rDd)
    //
    //    sample(sc)
    //
    //    union(sc)

//    intersection(sc)
  }

  /**
    * map()操作
    *
    * 将传入的函数应用于RDD中的每一条记录，返回由函数结果组成的新RDD。函数的结果值是一个对象，不是一个集合
    *
    * @param rdd
    */
  def map(rdd: RDD[String]): Unit = {
    //第一种形式
    val value = rdd.map(line => line.toUpperCase)
    value
      .foreach(x => println(x))
    //第二种形式
    //    for (x <- value) {
    //      println(x)
    //    }
  }

  /**
    * flatMap操作
    *
    * 与map()操作类似。但是传入flatMap()的函数可以返回0个、1个或者多个结果值。即函数结果值是一个集合，不是一个对象
    *
    * @param rdd
    */
  def flatMap(rdd: RDD[String]): Unit = {
    val value = rdd.flatMap(line => line.split(" "))
    value
      .foreach(x => println(x + "--->" + value.getClass.getTypeName))
  }

  /**
    * filter()函数返回一个新的RDD，只包含满足过滤条件的元素。这是一个窄依赖的操作不会将数据从一个分区转移到其他分区。
    * ---不会发生shuffle。
    * 例如，假设RDD包含5个整数(1、2、3、4和5)，过滤条件是判断是否偶数。过滤后得到的RDD将只包含偶数，即2和4。
    *
    * @param rdd
    */
  def filter(rdd: RDD[String]): Unit = {
    val unit = rdd.filter(x =>
      x.contains("间")
    )

    unit.foreach(x => println(x + "--->" + unit.getClass.getTypeName))
  }

  /**
    * 返回RDD中的非重复记录。注意：此操作是昂贵的，因为他需要对数据进行shuffle
    *
    * @param rdd
    */
  def distinct(rdd: RDD[String]): Unit = {
    rdd.distinct(1)
      .foreach(println)
  }


  /**
    * 在mapPartition()函数中，map()函数同时应用于每个partition分区。对比学习foreachPartition()函数，
    * foreachPartition()是一个action，操作方式与mapPartition相同
    */
  def mapPartitions(rdd: RDD[String]): Unit = {
    ///map每一个分区，然后再map分区中的每一个元素
    rdd.mapPartitions(partitions => {
      partitions.map(line => line.toLowerCase)
    }).foreach(x => {
      println("mapPartitions--->" + x)
    })
  }

  /**
    * 除了mapPartition外，它还为传入的函数提供了一个整数值，表示分区的索引，map()在分区索引上依次应用。
    *
    * @param rdd
    */
  def mapPartitionWithIndex(rdd: RDD[String]): Unit = {
    val value = rdd.mapPartitionsWithIndex((index, partition) => {
      partition.map(line => "索引为\t" + index + "-->" + line.toLowerCase)
    })
    value.foreach(println)

    value.partitions.foreach(println)
    println(value.partitioner)
  }

  /**
    * 采样操作
    *
    *
    * withReplacement=>,这个值如果是true时,采用PoissonSampler取样器(Poisson分布),
    *
    * 否则使用BernoulliSampler的取样器.
    *
    * Fraction=>,一个大于0,小于或等于1的小数值,用于控制要读取的数据所占整个数据集的概率.
    *
    * Seed=>,这个值如果没有传入,默认值是一个0~Long.maxvalue之间的整数.
    */

  def sample(sc: SparkContext): Unit = {

    val data = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 0)
    val value = sc.parallelize(data)
    println("----------------First-------------------")
    value.sample(withReplacement = true, 0.4, System.currentTimeMillis())
      .foreach(x => println(x))

    println("----------------Second-------------------")
    value.sample(withReplacement = false, 0.6, System.currentTimeMillis())
      .foreach(x => println(x))
  }

  /**
    * union 并集操作
    *
    * 使用union()函数，我们可以在新的RDD中获得两个RDD的元素。这个函数的关键规则是两个RDDs应该属于同一类型
    *
    * @param sc
    */
  def union(sc: SparkContext): Unit = {
    val d1 = List("spark", "hadoop", "zookeeper")
    val d2 = List("hive", "hbase", "kafka", "sqoop")
    val d3 = List(1, 2, 3, 4, 5)

    val r1 = sc.parallelize(d1)
    val r2 = sc.parallelize(d2)
    val r3 = sc.parallelize(d3)

    val value = r2.union(r1)
    value
      .foreach(println)
  }

  /**
    * 交集
    *
    * 使用intersection()函数，我们只得到新RDD中两个RDD的公共元素。这个函数的关键规则是这两个RDDs应该是同一类型的。
    */
  def intersection(sc: SparkContext): Unit = {
    val d1 = List("spark", "hadoop", "zookeeper", "hbase", "kafka")
    val d2 = List("hive", "hbase", "kafka", "sqoop")

    val r1 = sc.parallelize(d1)
    val r2 = sc.parallelize(d2)

    val value = r2.intersection(r1)
    value
      .foreach(println)
  }
}
