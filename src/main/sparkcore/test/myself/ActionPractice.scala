package test.myself

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * action 操作
  */
object ActionPractice {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("ActionPractice").setMaster("local")

    val sc = new SparkContext(conf)
    val value = sc.textFile("in/国内航班数据500条.csv")

    println(value.partitions)
    println(value.partitions.size)
    println(value.partitioner)


    //    count(value)
    //
    //    take(value)
    //    first(value)
    //
    //    takeOrdered(value)
    //
    //    top(value)
    //
    //    countByValue(sc)
    //
    //    reduce(sc)
    //
    //    foreachFunction(value)
  }

  /**
    * count()
    *
    * count()返回RDD中的元素数量
    * spark读取数据以行为单位进行的，即count为行数
    *
    * @param rdd
    */
  def count(rdd: RDD[String]): Unit = {
    val l = rdd.count()
    println("line is " + l)
  }

  /**
    * take()
    *
    * 从RDD返回n个元素。它试图减少它访问的分区数量，不能使用此方法来控制访问元素的顺序
    *
    * 用于获取RDD中从0到num-1下标的元素，不排序
    *
    * @param rdd
    */
  def take(rdd: RDD[String]): Unit = {
    rdd.take(10).foreach(println)
  }

  /**
    * takeOrdered()
    *
    * takeOrdered和top类似，只不过以和top相反的顺序返回元素
    *
    * @param rdd
    */
  def takeOrdered(rdd: RDD[String]): Unit = {
    rdd.map(line => line.split(",")(7))
      .takeOrdered(10)
      .foreach(println)
  }

  /**
    * first（）
    * 返回数据集的第一个元素(类似于take(1))
    *
    * @param rdd
    */
  def first(rdd: RDD[String]): Unit = {
    rdd.first()
      .foreach(print)
  }

  /**
    * top()
    *
    * 返回指定的RDD中定义的最大k元素
    *
    * 如果RDD中元素有序，那么可以使用top()从RDD中提取前几个元素
    *
    * 按照默认（降序）或者指定的排序规则，返回前num个元素
    *
    * @param rdd
    */
  def top(rdd: RDD[String]): Unit = {
    //默认排序
    rdd.map(line => line.split(",")(7))
      .top(10)
      .foreach(println)
    //指定排序
    rdd.map(line => line.split(",")(1).toFloat)
      .top(5)(Ordering.Float.reverse)
      .foreach(println)
  }

  /**
    * countByValue()
    *
    * countByValue()返回，每个元素都出现在RDD中的次数
    * 只在类型为(K, V)的RDDs上可用。返回一个(K, Int)对的hashmap和每个键的计数。
    *
    * To handle very large results, consider using
    *
    * {{{
    * rdd.map(x => (x, 1L)).reduceByKey(_ + _)
    * }}}
    *
    * @param sc
    */
  def countByValue(sc: SparkContext): Unit = {
    val value = sc.textFile("in/test.csv")
    value
      .flatMap(x => x.split(","))
      .countByValue()
      .take(10)
      .foreach(println)
  }

  /**
    * reduce()函数将RDD的两个元素作为输入，然后生成与输入元素相同类型的输出。这种函数的简单形式是一个加法
    *
    * collect()是将整个RDDs内容返回给driver程序的常见且最简单的操作。collect()的应用是单元测试，
    * 在单元测试中，期望整个RDD能够装入内存。如果使用了collect方法，但是driver内存不够，则内存溢出
    *
    * @param sc
    */
  def reduce(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(List(20, 32, 45, 62, 8, 5))
    val sum = rdd1.reduce(_ + _)
    println(sum)
  }


  /**
    *
    * 在foreach中，传入一个function，这个函数的传入参数就是每个partition中，每次的foreach得到的一个rdd的kv实例，也就是具体的内容，
    * 这种处理你并不知道这个iterator的foreach什么时
    * 候结束，只能是foreach的过程中，你得到一条数据，就处理一条数据。
    *
    *
    * foreachPartition函数也是根据传入的function进行处理，但不同处在于，这里function的传入参数是一个partition对应数据的iterator，
    * 而不是直接使用iterator的foreach
    *
    *
    * foreach 返回的是具体的每条数据  而foreachPartition返回的则是每个partition的iterator
    *
    *
    * 在实践中发现，foreachPartitions类的算子，对性能的提升还是很有帮助的。比如在foreach函数中，将RDD中所有数据写MySQL，那么如果
    * 是普通的foreach算子，就会一条数据一条数据地写，每次函数调用可能就会创建一个数据库连接，此时就势必会频繁地创建和销毁数据库连接，
    * 性能是非常低下；但是如果用foreachPartitions算子一次性处理一个partition的数据，那么对于每个partition，只要创建一个数据库连
    * 接即可，然后执行批量插入操作，此时性能是比较高的
    */


  def foreachFunction(rdd: RDD[String]): Unit = {

    val list = new ArrayBuffer[String]()
    rdd.foreach(x => {
      list += x
      if (list.size > 100) {
        println(x)
      }
    })

    val list1 = new ArrayBuffer[String]()
    rdd.foreachPartition(it => {
      it.foreach(x => {
        list1 += x
        if (list1.size > 1000)
          println("foreach-----" + x)
      })
      if (list1.nonEmpty)
        println("foreachPartition-----")
    })

  }
}
