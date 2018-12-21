package sparksql.homework

import java.io.File

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 1、读取zips.json文件为DataFrame，并将列名_id重命名为zip
  * 2、创建名为Zips的case class或者javaBean，用于将第一步创建的DF转换为DS
  * 3、显示DS中的数据
  * *
  * 数据分析：
  * 1、以降序显示人口超过40000的 states, zip, cities,pop
  * 2、显示名为CA的states中人口最多的三个城市
  * 3、把所有州states的人口加起来，按降序排列,显示前10名
  * *
  * 请使用sql语句和Dataset API两种方式完成上述数据分析内容
  */
object ZipProblemSolution {
  private val warehouseLocation = new File("spark-warehouse").getAbsolutePath

  case class Zip(zip: String, city: String, loc: String, pop: Long, state: String)

  case class Zips(zip: String, city: String, loc: Array[Double], pop: Long, state: String)

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().appName("ZipProblemSolution")
      .master("local[*]")
      //      .config("spark.sql.warehouse.dir", warehouseLocation)
      //      .enableHiveSupport()
      .getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")

    //读取zips.json文件为DataFrame，并将列名_id重命名为zip
    val zipDf = ss.read.format("json").json("in/zips.json").withColumnRenamed("_id", "zip")
    //    zipDf.printSchema()
    //    //缓存数据，提高性能
    //    zipDf.cache()
    //
    //    val zipRDD = zipDf.rdd.map(row => {
    //      val zip: String = row.getAs("zip")
    //      val city: String = row.getAs("city")
    //      val loc: Seq[Double] = row.getAs("loc")
    //      val pop: Long = row.getAs("pop")
    //      val state: String = row.getAs("state")
    //      println("loc:" + loc.mkString(","))
    //      Zip(zip, city, loc.mkString(","), pop, state)
    //    })
    //    import ss.implicits._
    //    val zipRDDDS = zipRDD.toDS()
    //
    //    val ds = zipDf.as[Zip]
    //    //    ds.show()
    //
    //
    //    //数据分析 1
    //    ds.createOrReplaceTempView("zips")

    //    ss.sql("select state,zip,city,pop from zips where pop>40000 order by pop desc").show()
    //    //1、Spark Sql:以降序显示人口超过40000的 state, zip, city,pop
    //    ds.select("state", "zip", "city", "pop").where(expr("pop > 40000")).orderBy(desc("pop")).show()
    //    //2、显示名为CA的states中人口最多的三个城市
    //    ss.sql("select * from zips where state='CA' order by pop desc limit 3 ").show()
    //    ds.select("*").where("state = 'CA'").orderBy(desc("pop")).limit(3).show()
    //    //把所有州states的人口加起来，按降序排列,显示前10名
    //    ss.sql("select state,sum(pop) from zips group by state order by sum(pop) desc").show(10)
    //    ss.sql("select state,sum(pop) from zips group by state order by sum(pop) desc limit 10").show()
    //    ds.select("state", "pop").groupBy("state").sum("pop").orderBy(desc("sum(pop)")).show(10)
    //    ds.select("state", "pop").groupBy("state").sum("pop").orderBy(desc("sum(pop)")).limit(10).show()

    //聚合函数
    //计算名为CA的state，每个city的zip总数、人口总量
    //    ss.sql("select * from zips where city='NOVATO'").show()
    //    ss.sql("select count(zip),city,sum(pop) from zips where state='CA' group by city").show()
    //    ds.select("zip", "pop", "city").filter("state='CA'").groupBy("city").agg(count("zip").alias("zipcount"), sum("pop").alias("popsum")).show()

    //    ds.filter('state === "CA").select("zip", "pop", "city").groupBy("city").agg(sum("pop").alias("popsum"), count("zip").alias("zipcount")).orderBy(desc("popsum")).explain(true)

    /**
      *
      * UDF的定义及使用
      * 如果spark内置函数不够用，那么可以自定义函数(UDF---User Defined Function)
      * 注册一个UDF，用于将string类型的zip改为long类型
      * spark.udf.register("zipToLong", (z:String) => z.toLong)
      * 1、编写的UDF可以放到SQL语句的select部分，也可以作为where、groupBy或者having子句的一部分。
      * 2、UDF是一个函数，但是UDF不仅仅是一个函数，有自己的特殊性：需要将UDF的参数看做数据表的某个列
      * 3、在使用UDF时，不一定非要传入列，还可以传入常量
      * spark.udf.register("largerThan", (z:String,number:Long) => z.toLong>number)
      * 4、使用Dataset API和UDF的时候，上述注册方法，对DataFrame API不可见，改为下面的方法：
      * val zipToLongUDF=udf((z:String) => z.toLong)
      * val largerThanUDF = udf((z: String, number: Long) => z.toLong>number)
      * zipDS.select(col("city"),zipToLongUDF(col("zip")).as("zipToLong"),largerThanUDF(col("zip"),lit("99923")).alias("largerThan")).orderBy(desc
      * ("zipToLong")).show();
      **/
    //sql
    //    ss.udf.register("zipToLong", (z: String) => z.toLong)
    //    ss.udf.register("largeThan", (z: String, num: Long) => z.toLong > num)
    //    //    ss.sql("select city,zipToLong(zip) as zipToLong from zips order by zipToLong desc").show()
    //    //    ss.sql("select city,zipToLong(zip) as zipToLong,largeThan(zip,99820) as large_than from zips order by zipToLong desc").show()
    //    //api
    import org.apache.spark.sql.functions._
    val zipToLong = udf((z: String) => z.toLong)
    //    val largeThan = udf((z: String, num: Long) => z.toLong > num)
    //
    //    zipDf.select(col("city"), zipToLong(col("zip")).as("zipToLong"), largeThan(col("zip"), lit("99820")).alias("large_than")).orderBy(desc("zip")).show()

    /**
      * Spark On Hive:
      * Spark On Hive与Hive On Spark
      * Spark On Hive:
      * 1、是否需要启动hive?--不需要
      * 2、是否需要启动HDFS？---需要
      * 3、是否需要启动YARN？--不需要
      * 4、如何配置Spark On Hive？
      * 5、Spark On Hive的内部机制是什么？
      * 6、Spark On Hive实战：com.brave.prepare.SparkSqlHiveTest
      * 7、将作业数据保存到hive中
      */
    zipDf.write.mode(SaveMode.Overwrite).saveAsTable("hive_zips_table")
    ss.sql("show tables").show()
    ss.sql("select * from hive_zips_table").show()
    ss.close()
    //    import ss.sql
    //    sql("show databases").show()
    //    sql("use spark")
    //    sql("show tables").show()
  }
}
