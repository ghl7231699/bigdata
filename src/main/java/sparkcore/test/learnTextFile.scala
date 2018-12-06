package sparkcore

import org.apache.spark.{SparkConf, SparkContext}

object learnTextFile {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    conf.setAppName("learnTextFile")

    /**
      * local:一个线程
      * local[*]:服务器core数量的相差
      * local[4]:4个线程
      */
    conf.setMaster("local")

    val sc =new SparkContext(conf)

    val textFileRDD=sc.textFile("in/README.md")
//    val textFileRDD=sc.textFile("hdfs://bigdata01:9000/sparkdata2")
    val mapRDD=textFileRDD.map(line => (line,line.size))
    mapRDD.map(t =>(t._2,t._1)).top(6).foreach(t =>println(t._1+" : "+t._2))
   println( mapRDD.toDebugString)
//    val count=textFileRDD.count()
//    println("count:"+count)
//    val uppercaseRDD=textFileRDD.map(line => line)
//    for (elem <- uppercaseRDD.take(3)) {
//      println(elem)
//    }
//    textFileRDD.map(line => line.toUpperCase()).saveAsTextFile("hdfs://bigdata01:9000/sparkdata2")
//    val flatMapRDD=textFileRDD.flatMap(line =>line.split(" "))
//    flatMapRDD.take(3).foreach(word => println(word))
//    println("count:"+count)
//
//    val rdd1 = sc.parallelize(Seq((1,"jan",2016),(3,"nov",2014),(16,"feb",2014)))
//    val rdd2 = sc.parallelize(Seq((5,"dec",2014),(17,"sep",2015)))
//    val rdd3 = sc.parallelize(Seq((6,"dec",2011),(16,"may",2015)))
//    val rddUnion = rdd1.union(rdd2).union(rdd3)
//    rddUnion.foreach(println)
    sc.stop();
  }
}
