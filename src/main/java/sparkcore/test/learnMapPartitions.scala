package sparkcore

import org.apache.spark.{SparkConf, SparkContext}

object learnMapPartitions {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    conf.setAppName("learnMapPartitions")
    conf.setMaster("local[4]")

    val sc =new SparkContext(conf)
    val textFileRDD=sc.textFile("in/国内航班数据说明.txt")

    //map每一个分区，然后再map分区中的每一个元素
    val mapPartitionRDD=textFileRDD.mapPartitions(partition => {
      partition.map(line =>line.toUpperCase)
    })

    //foreach是一个没有返回值的action
//    mapPartitionRDD.foreach(line =>println(line))

    val mapPartitionsWithIndexRDD=textFileRDD.mapPartitionsWithIndex((index,partition) => {
      partition.map(line =>index +" : "+line.toUpperCase)
    })

//    mapPartitionsWithIndexRDD.foreach(line =>println(line))

    val data = sc.parallelize(Array(('A',1),('b',2)))
    val data2 =sc.parallelize(Array(('A',4),('A',6),('b',7),('c',3),('c',8)))
    val result = data.join(data2)
    //(A,(1,4)),(A,(1,6)),(b,(2,7))
    println(result.collect().mkString(","))
    /**
      * 输出：
      * A:(1,4)
      * A:(1,6)
      * b:(2,7)
      */
    result.foreach(t =>println(t._1+":("+t._2._1+","+t._2._2+")"))
  }
}
