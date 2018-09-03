package test.myself

import org.apache.spark.{SparkConf, SparkContext}

object PairRddFromTupleList {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("create").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val tuple = List(("zhangsan","spark"), ("lisi","kafka"), ("Mary","hadoop"), ("James","hive"))
    val pairRDD = sc.parallelize(tuple)
    val filterRDD=pairRDD.filter(t =>t._2.equals("spark"))
    filterRDD.foreach(t =>println(t._1+","+t._2))
  }
}
