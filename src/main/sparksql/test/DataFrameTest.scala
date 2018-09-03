package test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * DataFrame
  */
object DataFrameTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("DataFrameTest").setMaster("local")
    val sc = new SparkContext(conf)



    val range =sc.range(0,1000)
//    range.toDF("number")
  }

}
