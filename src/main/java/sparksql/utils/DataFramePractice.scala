package sparksql.utils

import java.sql.Date

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * sparkSql
  * 三种方式创建dataFrame
  *
  */
object DataFramePractice {

  case class human(name: String, age: Int, city: String, date: Date)


  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder().appName("dataFrameTest").master("local").getOrCreate()
    builder.sparkContext.setLogLevel("ERROR")

    defaultToDataFrame(builder)

    objectToDateFrame(builder)

    rddToDataFrame(builder)

    schemaToDataFrame(builder)

    jsonToDataFrame(builder)

  }

  private def defaultToDataFrame(builder: SparkSession): Unit = {
    import builder.implicits._
    /**
      * import ***.implicits._   ***为sparkSession变量名
      * 导入隐式转换函数，可以将本地序列(seq), 列表或者RDD转为DataFrame
      * 可以使用toDF("name","age","address","birthday")指定列名
      * 如果不指定列名，spark默认设置列名为：_1,_2,_3,_4
      */

    val seq = Seq(
      ("zhangsan", 10, "beijing", java.sql.Date.valueOf("2008-01-01")),
      ("lisi", 20, "shanghai", java.sql.Date.valueOf("1998-01-01")),
      ("wangwu", 21, "chongqing", java.sql.Date.valueOf("1997-01-01")),
      ("zhaoliu", 25, "baoding", java.sql.Date.valueOf("1993-01-01")),
      ("tianqi", 19, "hangzhou", java.sql.Date.valueOf("1991-01-01"))
    )

    val frame = seq.toDF("name", "age", "city", "date")

    frame.show()
    frame.printSchema()

    frame.createOrReplaceTempView("men")

    val result = builder.sql("select * from men where age>20")
    result.show()
  }

  private def objectToDateFrame(builder: SparkSession): Unit = {
    import builder.implicits._
    val humans = Seq(human("zhao", 10, "guangzhou", Date.valueOf("1990-10-02")), human("qian", 20, "guangzhou", Date.valueOf("1990-10-03")), human("sun", 30, "guangzhou", Date.valueOf("1990-10-04")), human("li", 40, "guangzhou", Date.valueOf("1990-10-05")))

    println("*********objectToDateFrame*********")
    val unit = humans.toDF()
    unit.show()
    unit.printSchema()
    unit.createOrReplaceTempView("human")
    builder.sql("select name from human where age>10").show()
  }

  def rddToDataFrame(builder: SparkSession): Unit = {
    import builder.implicits._
    println("*********rddToDataFrame*********")
    val seq = Seq(
      ("zhangsan", 10, "beijing", java.sql.Date.valueOf("2008-01-01")),
      ("lisi", 20, "shanghai", java.sql.Date.valueOf("1998-01-01")),
      ("wangwu", 21, "chongqing", java.sql.Date.valueOf("1997-01-01")),
      ("zhaoliu", 25, "baoding", java.sql.Date.valueOf("1993-01-01")),
      ("tianqi", 19, "hangzhou", java.sql.Date.valueOf("1991-01-01"))
    )
    val unit = builder.sparkContext.parallelize(seq, 2)
    unit.toDF().show()
    unit.toDF().printSchema()
  }

  /**
    * 使用createDataFrame(rdd,schema)方法创建DataFrame
    */
  private def schemaToDataFrame(builder: SparkSession): Unit = {
    println("*********schemaToDataFrame*********")
    val schema = StructType(List(
      StructField("name", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false),
      StructField("address", StringType, nullable = false),
      StructField("birthday", DateType, nullable = false)
    ))

    val value = builder.sparkContext.parallelize(Seq(Row("zhangsan", 10, "beijing", java.sql.Date.valueOf("2008-01-01")), Row("tianqi", 19, "hangzhou", java.sql.Date.valueOf("1991-01-01"))))

    val frame = builder.createDataFrame(value, schema)
    frame.show()
    frame.printSchema()
  }

  /**
    * 通过json scv DB等文件创建
    *
    * @param builder sparksession
    */
  def jsonToDataFrame(builder: SparkSession): Unit = {
    println("***********jsonToDataFrame**************")
    val json = builder.read.json("in/people.json")
    json.show()
    json.printSchema()
  }

}
