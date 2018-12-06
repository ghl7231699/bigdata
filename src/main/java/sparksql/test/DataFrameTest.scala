package sparksql.test

import java.sql.Date

import org.apache.spark.sql.SparkSession

/**
  * DataFrame
  */
object DataFrameTest {

  case class Male(name: String, age: Int, address: String, date: Date)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataFrameTest").master("local").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    /**
      * import spark.implicits._
      * 导入隐式转换函数，可以将本地序列(seq), 列表或者RDD转为DataFrame
      * 可以使用toDF("name","age","address","birthday")指定列名
      * 如果不指定列名，spark默认设置列名为：_1,_2,_3,_4
      */
    val seq = Seq(
      ("zhangsan", 1, "beijing", Date.valueOf("2018-10-10")),
      ("lisi", 28, "shanghai", Date.valueOf("1990-10-10"))
    )

    val frame = seq.toDF("name", "age", "address", "date")
    frame.show()
    frame.printSchema()


    /**
      * 创建序列时，直接使用case class作为序列元素，则toDF()时，不需要指定列名。
      */
    println("*********case class seq*********")

    val male = Seq(
      Male("张三", 28, "保定", Date.valueOf("2018-09-06")),
      Male("李四", 10, "石家庄", Date.valueOf("2014-06-23"))
    )
    val value = male.toDF()
    value.show()
    value.printSchema()

    intent(male, spark)

    textFile(spark)

  }

  /**
    * 通过读取json文件
    */
  private def textFile(spark: SparkSession) = {
    spark.read.json("in/people.json")
  }

  /**
    * 通过隐式转换函数，可以将RDD转换为DataFrame
    */

  def intent(seq: Seq[Male], spark: SparkSession): Unit = {
    import spark.implicits._
    println("************RDD To DataFrame**************")

    val rdd = spark.sparkContext.parallelize(seq)
    val frame = rdd.toDF()
    frame.show()
    frame.printSchema()
  }
}
