package homework

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 1、航班数最多的航空公司，算出前6名
  * 2、北京飞往重庆的航空公司，有多少个？
  */
object FlightFilter {


  def main(args: Array[String]): Unit = {
    executeFlightJob()
  }

  def executeFlightJob(): Unit = {
    val conf = new SparkConf()
    conf.setAppName("FlightFilter")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val flightRDD = sc.textFile("in/国内航班数据500条.csv")
    //    val flightRDD = sc.textFile("hdfs://ghl01:8020/user/ghl/source/国内航班数据500条.csv")

    //获取航班数最多的前6位
    val value = flightRDD.map(line => {
      val ops = line.split(",")
      ((ops(8), ops(7)), 1)
    })
      .reduceByKey((x, y) => x + y)
      .groupBy(_._1._1)
      .mapValues(_.size)
      .map(z => (z._2, z._1))
      .sortByKey(ascending = false)
      .top(6)
      .map(x => (x._2, x._1))
      value.foreach(println)
//    value.foreachPartition(insert)

    //    upload(value, "hdfs://ghl01:8020/user/ghl/ranking")

    //    北京飞往重庆的航班
    flightRDD.map(line => {
      val ops = line.split(",")
      ((ops(0), ops(3)), ops(8))
    }).filter(line => line._1._1.equals("北京") && line._1._2.equals("重庆"))
      .map(x => (x._2, 1))
      .reduceByKey((x, y) => x + y)
      .foreach(println)

    sc.stop()


  }

  private def upload(value: RDD[(String, Int)], path: String) = {
    value.saveAsTextFile(path)
  }


  case class flight(airline: String, number: Int)

  def insert(iterator: Iterator[(String, Int)]) = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into rank(airline,number) values(?,?)"
    val host = "jdbc:mysql://ghl01:3306/homework?useUnicode=true&characterEncoding=utf-8"

    try {
      conn = DriverManager.getConnection(host, "root", "123456")
      iterator.foreach(x => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, x._1)
        ps.setInt(2, x._2)
        ps.executeLargeUpdate()
      })
    } catch {
      case e: Exception => println("Mysql Exception " + e)
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }
}
