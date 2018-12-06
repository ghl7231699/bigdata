package test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 共享变量  广播
  */
object LearnBroadcast {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("Secondary Sort")
    val sc = new SparkContext(conf)

    var list1 = List(("zhangsan", 20), ("lisi", 25), ("wangwu", 30))
    var list2 = List(("zhangsan", "kafka"), ("lisi", "spark"), ("zhaoliu", "hive"))

    val rdd1 = sc.parallelize(list1)
    val rdd2 = sc.parallelize(list2)
    //此方法有性能问题，会引起shuffle,如何优化====>共享变量
    rdd1.join(rdd2).foreach(s => println(s._1 + ":" + s._2._1 + "\t" + s._2._2))
    //通过广播将普通的join转换为map-side join
    val map = rdd2.collectAsMap()
    val result = sc.broadcast(map)

    rdd1.mapPartitions(partition => {
      val v = result.value
      for {(key, value) <- partition
           if map.contains(key)
      } yield (key, v.getOrElse(key, ""), value)
    }).foreach(t => println(t))
  }

}
