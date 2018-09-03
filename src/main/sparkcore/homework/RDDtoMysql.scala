package homework

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * 将RDD数据写入mysql
  */
object RDDtoMysql {

  def main(args: Array[String]): Unit = {

  }

  case class flight(airline: String, number: Int)

  def insert(iterator: Iterator[(String, Int)]) = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into flight(airline,number) values(?,?)"
    val host = "jdbc:mysql://ghl01:3306/homework"

    try {
      conn = DriverManager.getConnection(host, "root", "123456")
      iterator.foreach(x => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, x._1)
        ps.setInt(2, x._2)
        ps.executeLargeUpdate()
      })
    } catch {
      case e: Exception => println("Mysql Exception" + e)
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
