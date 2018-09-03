package test.myself

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * PairRDD练习
  */
object PairRddPractice {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("PairRddPractice")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    val data = List((100, "spark"), (1112, "hadoop"), (1113, "hive"), (49, "hbase"), (15, "kafka"), (10, "stream"))

    val unit = sc.parallelize(data)
    unit.foreach(x => println(x._1 + "--" + x._2))

    val d1 = List("spark hadoop", "hive hbase", "kafka stream")
    val unit1 = sc.parallelize(d1)
    unit1.map(x => (x.split(" ")(0), x.split(" ")(1)))
      .foreach(line => println(line._1 + " " + line._2))

    unit.filter(x => x._2.equals("spark"))
      .map(y => y._1)
      .foreach(println)

    reduceByKey(sc)

    sortKey(unit, sc)

    join(sc)

    combineBy(sc)

    partitions(sc)
  }

  /**
    * spark版的wordcount功能
    *
    */
  def reduceByKey(sc: SparkContext): Unit = {
    val value = sc.textFile("in/test.csv")

    value.flatMap(x => x.split(","))
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
      .foreach(println)
  }

  /**
    * 当我们在(K, V)数据集中应用sortByKey()函数时，数据是根据RDD中的键K排序的
    *
    * @param sc
    */
  def sortKey(rdd: RDD[(Int, String)], sc: SparkContext): Unit = {
    println("---------------------------------------------sortKey---------------------------------------------")
    rdd.sortByKey(false)
      .foreach(println)
  }

  /**
    * join是数据库术语。它使用公共值组合两个表中的字段。Spark中的join()操作是在pairRDD上定义的。
    * pairRDD每个元素都以tuple的形式出现。
    * tuple第一个元素是key，第二个元素是value。join()操作根据key组合两个数据集。
    *
    * 注意:join()根据key连接两个不同的RDDs。
    *
    * @param sc
    */
  def join(sc: SparkContext): Unit = {
    println("-------------------------------------------------join---------------------------------------------")
    val data = sc.parallelize(Array(('A', 1), ('b', 2)))
    val data2 = sc.parallelize(Array(('A', 4), ('A', 6), ('b', 7), ('c', 3), ('c', 8)))

    data.join(data2)
      .collect()
      .foreach(println)
  }

  case class ScoreDetail(name: String, subject: String, score: Float)

  /**
    *
    *
    * createCombiner为V => Iterable[V]。
    * mergeValue则将原RDD中Pair的Value合并为操作后的C类型数据。合并操作的实现决定了结果的运算方式。所以，mergeValue更像是声明了一种合并方式，
    * 它是由整个combine运算的结果来导向的。
    * 函数的输入为原RDD中Pair的V，输出为结果RDD中Pair的C。
    * 最后的mergeCombiners则会根据每个Key对应的多个C，进行归并。
    *
    * @param sc
    */
  def combineBy(sc: SparkContext): Unit = {
    val scores = List(
      ScoreDetail("lili", "Math", 98),
      ScoreDetail("lili", "English", 88),
      ScoreDetail("zhangsan", "Math", 75),
      ScoreDetail("zhangsan", "English", 78),
      ScoreDetail("wangliu", "Math", 90),
      ScoreDetail("wangliu", "English", 80),
      ScoreDetail("lisi", "Math", 91),
      ScoreDetail("lisi", "English", 80)
    )

    //生成ScoreDetail集合
    val sd = for (i <- scores) yield (i.name, i)
    //("lili",ScoreDetail)
    //转换成PairRdd
    val pad = sc.parallelize(sd)
    println("--------------------学生的个人平均分-----------------------")
    //计算平均分数
    pad.combineByKey(
      (x: ScoreDetail) => (x.score, 1), //(98,1)
      (acc: (Float, Int), x: ScoreDetail) => (acc._1 + x.score, acc._2 + 1), //(总分，科目数)
      (acc1: (Float, Int), acc2: (Float, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )
      .map({
        case (key, value) => (key, value._1 / value._2)
      })
      .collect()
      .foreach(println)

    val rdd = sc.parallelize(for (i <- scores) yield (i.subject, i))

    println("----------------单科成绩平均分-----------------")
    //计算学科的平均分
    rdd.combineByKey(
      (x: ScoreDetail) => (x.score, 1),
      (score: (Float, Int), s: ScoreDetail) => (score._1 + s.score, score._2 + 1),
      (acc1: (Float, Int), acc2: (Float, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )
      .map({
        case (k, v) => (k, v._1 / v._2)
      })
      .collect()
      .foreach(println)
  }

  /**
    *
    * @param sc
    */
  def partitions(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(Seq((1, "jan", 2016), (3, "nov", 2014), (16, "feb", 2014)), 1)
    val rdd2 = sc.parallelize(Seq((5, "dec", 2014), (17, "sep", 2015)), 1)
    val rdd3 = sc.parallelize(Seq((6, "dec", 2011), (16, "may", 2015)), 1)
    val rddUnion = rdd1.union(rdd2).union(rdd3)
    println("rddUnion.partitions.size:" + rddUnion.partitions.size)
    rddUnion.foreach(println)
  }

}
