package homework

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}


/**
  * Spark二次排序 * 对应数据源：employee_all.csv * 数据字段说明：
  * First Name 名
  * Last Name 姓
  * Job Titles职称
  * Department 部门
  * Full or Part-Time 全职或兼职
  * Salary 工资
  * Typical Hours 小时工
  * Annual Salary 年薪
  * Hourly Rate 时薪
  * ALLISON,PAUL W,LIEUTENANT,FIRE,F,Salary, ,107790.00,
  * 要求1：根据Department字段对数据进行分组，然后根据Annual Salary排序，然后根据firstName排序。
  * 要求2：将每个部门的工资最高的前三名，存入mysql数据库,数据库表包括如下字段(Department,Annual Salary,firstName)
  **/
object EmployeeFilter {

  def main(args: Array[String]): Unit = {
    executeEmployeeJob()
  }

  def executeEmployeeJob(): Unit = {

    val conf = new SparkConf()
    conf.setAppName("EmployeeFilter")
    conf.setMaster("local")


    val sc = new SparkContext(conf)
    //    val employee = sc.textFile("in/test.csv")
    val employee = sc.textFile(String.format(Constants.host, "/user/ghl/source/employee_all.csv"))

    //要求1
    val value = employee.map(line => {
      val lines = line.split(",")
      (lines(0), lines(1), lines(2), lines(3), lines(4), lines(5), lines(6), lines(7))
    }).groupBy(x => x._4)

    value.map(y => {
      y._2.toList
        .sortBy(_._8)
        .sortBy(_._1)
    })
      .collect()
      .foreach(println)

    //要求2
    val sorts = value.map(x => {
      x._2.toList
        .filter(z => {
          !"".equals(z._8)
        })
        .map(f =>
          (f._1, f._2, f._3, f._4, f._5, f._6, f._7, f._8.toFloat)
        )
        .sortBy(_._8)(Ordering.Float.reverse)
        .take(3)
        .map(y => {
          (y._4, y._8, y._1)
        })
    })

    sorts
      .collect()
      .foreach(println)

    sorts.foreachPartition(insert)

    sc.stop()

  }

  case class flight(department: String, annual_salary: String, first_name: String)

  def insert(iterator: Iterator[List[(String, Float, String)]]) = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into employee(department,annual_salary,first_name) values(?,?,?)"
    val host = "jdbc:mysql://ghl01:3306/homework?useUnicode=true&characterEncoding=utf-8"

    try {
      conn = DriverManager.getConnection(host, "root", "123456")
      for (i <- iterator) {
        ps = conn.prepareStatement(sql)
        i.foreach(line => {
          ps.setString(1, line._1)
          ps.setString(2, line._2.toString)
          ps.setString(3, line._3)
          ps.executeLargeUpdate()
        })


      }
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
