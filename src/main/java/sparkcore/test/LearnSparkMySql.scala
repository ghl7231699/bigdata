package sparkcore

import java.sql.PreparedStatement

import org.apache.spark.{SparkConf, SparkContext}
import utils.JdbcUtils

/**
  * 使用package打包，provided不会打包进去
  * ./spark-submit --class sparkcore.LearnSparkMySql --master spark://bigdata01:7077 /opt/sparkapp/spark-1.0-SNAPSHOT-jar-with-dependencies.jar
  */
object LearnSparkMySql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("FlightFilter")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val textFileRDD = sc.textFile("hdfs://ghl01:8020/user/ghl/source/")

    val mapRDD = textFileRDD.map(line => line.length)
    //第一种方式
    mapRDD.foreach(lineSize => {
      val conn = JdbcUtils.getConnection
      val sql = "insert into textFile (value) values(?)"
      var pst: PreparedStatement = null
      pst = conn.prepareStatement(sql)
      pst.setString(1, lineSize + "")
      pst.executeUpdate
      JdbcUtils.free(pst, conn)
    })

    mapRDD.foreachPartition(partitions => {

      val conn = JdbcUtils.getConnection
      var pst: PreparedStatement = null
      partitions.foreach(lineSize => {
        val sql = "insert into textFile (value) values(?)"
        pst = conn.prepareStatement(sql)
        pst.setString(1, lineSize + "")
        pst.executeUpdate

      })
      JdbcUtils.free(pst, conn)
    })

  }

}
