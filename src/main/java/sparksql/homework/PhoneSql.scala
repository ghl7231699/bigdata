package sparksql.homework

import java.io.File

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * SparkSQL大数据分析案例：天猫-商品列表(手机)数据分析  scala版本
  *
  * 分析数据的字段中文名称："店铺名称","产品名称","产品价格","产品销量","商品链接","产品评价"
  */
object PhoneSql {
  private val warehouseLocation = new File("spark-warehouse").getAbsolutePath

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .master("local[*]")
      .appName("PhoneSql")
      //            .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    /**
      * val data = spark.read.option("inferSchema", "true").option("header", "false") //这里设置是否处理头信息，
      * false代表不处理，也就是说文件       的第一行也会被加载进来，如果设置为true，那么加载进来的数据中不包含第一行，第一行被当作了头信息
      * ，也就是表中的字段名处理了.csv(s"file:///home/spark/file/project/${i}visit.txt")　　
      * //这里设置读取的文件，${i}是我引用的一个变量，如果要在双引号之间引用变量的话，括号前面的那个s不能少
      * .toDF("mac", "phone_brand", "enter_time", "first_time", "last_time", "region", "screen", "stay_time")
      * //将读进来的数据转换为DF，并为每个字段设置字段名
      */

    val frame = session.read.option("header", "true").format("csv").csv("in/phone.csv")
    //        frame.show()
    frame.printSchema()

    import org.apache.spark.sql.functions._
    //获取手机品牌
    session.udf.register("splitCol", (s: String) => s.split("/")(0))
    session.udf.register("price", (s: String) => s.replaceAll("¥", "").toFloat)
    session.udf.register("number", (s: String) => {
      if (s == null) {
        0f
      } else {
        if (s.contains("该款月成交 ")) {
          val str = s.replaceAll("该款月成交 ", "")
          if (str.contains("万笔")) {
            str.replaceAll("万笔", "0000").toFloat
          } else if (s.contains("笔")) {
            str.replaceAll("笔", "").toFloat
          } else {
            0f
          }
        } else {
          0f
        }
      }
    })

    val price = udf((s: String) => s.replaceAll("¥", "").toFloat)

    val number = udf((s: String) => {
      if (s == null) {
        0f
      } else {
        if (s.contains("该款月成交 ")) {
          val str = s.replaceAll("该款月成交 ", "")
          if (str.contains("万笔")) {
            str.replaceAll("万笔", "0000").toFloat
          } else if (s.contains("笔")) {
            str.replaceAll("笔", "").toFloat
          } else {
            0f
          }
        } else {
          0f
        }
      }
    })
    frame.createOrReplaceTempView("phones")
    //价格最贵的手机
    session.sql("select cpmc,price(cpjg) as price from phones order by price(cpjg) desc").show(10)
    //获取销量最高的手机10
    frame.select(col("cpmc"), number(col("cpxl")).as("sales_volume")).orderBy(desc("sales_volume")).limit(10).show()

    frame.write.mode(SaveMode.Overwrite).saveAsTable("phones")
    session.sql("show tables").show()
    session.sql("select * from phones").show()
    session.close()
  }
}
