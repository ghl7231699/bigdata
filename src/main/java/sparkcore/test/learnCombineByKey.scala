package sparkcore

import org.apache.spark.sql.SparkSession

object learnCombineByKey {
  /**
    *
    */
  case class ScoreDetail(studentName:String,subject:String,score:Float)
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("rddCreate").master("local").getOrCreate()
    val sc=spark.sparkContext
    /**
      * https://www.edureka.co/blog/apache-spark-combinebykey-explained
      *
      * CombineByKey transformation
      * CombineByKey API有三个函数(如Python中的lambda表达式或Scala中的匿名函数)，即：
      * Create combiner function: x
      * Merge value function: y
      * Merge combiners function: z
      *
      * API格式为combineByKey(x, y, z)。
      * 让我们看一个例子(Scala语言)：本例的目标是找到每个学生的平均分数。
      */

    val scores=List(
      ScoreDetail("A","Math",98),
      ScoreDetail("A","English",88),
      ScoreDetail("B","Math",75),
      ScoreDetail("B","English",78),
      ScoreDetail("C","Math",90),
      ScoreDetail("C","English",80),
      ScoreDetail("D","Math",91),
      ScoreDetail("D","English",80)
    )

    /**
      *将测试数据转换为键值对形式--键key为学生名称Student Name，值为ScoreDetail实例对象
      */
    val scoresWithKey=for(i <- scores)yield (i.studentName , i)

    /**
      * 创建一个pairRDD
      */
    val scoreWithKeyRDD=sc.parallelize(scoresWithKey)

    /**
      *计算平均分数
      */
    val avgScoresRDD=scoreWithKeyRDD.combineByKey(
      (x:ScoreDetail) => (x.score,1),
      (acc:(Float,Int),x:ScoreDetail) => (acc._1+x.score,acc._2+1),
      (acc1:(Float,Int),acc2:(Float,Int)) => (acc1._1+acc2._1,acc1._2+acc2._2)
    ).map({
      case (key,value) =>(key,value._1/value._2)
    })

    avgScoresRDD.collect().foreach(println)


    scoreWithKeyRDD.combineByKey(
      (x:ScoreDetail)=>(x.score,1),
      (acc:(Float,Int),x:ScoreDetail) => (acc._1+x.score,acc._2+1),
      (acc1:(Float,Int),acc2:(Float,Int)) => (acc1._1+acc2._1,acc1._2+acc2._2)
    )

  }
}

