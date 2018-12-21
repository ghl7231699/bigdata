package com.brave.sparksql

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author brave
  */
object ZipProblemSolution {

  case class Zips(zip: String, city: String, loc: Array[Double], pop: Long, state: String)

  case class Zips2(zip: String, city: String, loc: String, pop: Long, state: String)

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.functions._
    val spark = SparkSession.builder()
      .appName("ZipProblemSolution")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    //1、读取zips.json文件为DataFrame，并将列名_id重命名为zip
    val zipDF = spark.read
      .json("in/zips.json")
      .withColumnRenamed("_id", "zip")

    //    zipDF.printSchema()
    //后面频繁用到这一份数据，所以缓存起来，提高性能
    zipDF.cache()
    //    zipDF.map(row =>{
    //      val zip:String=row.getAs("zip")
    //      val city:String=row.getAs("city")
    //      val loc:Seq[Double]=row.getAs("loc")
    //      val pop:Long=row.getAs("pop")
    //      val state:String=row.getAs("state")
    //      println("loc:"+loc.mkString(","))
    //      Zips2(zip,city,loc.mkString(","),pop,state)
    //    }).show()

    val zipRDD = zipDF.rdd.map(row => {
      val zip: String = row.getAs("zip")
      val city: String = row.getAs("city")
      val loc: Seq[Double] = row.getAs("loc")
      val pop: Long = row.getAs("pop")
      val state: String = row.getAs("state")
      println("loc:" + loc.mkString(","))
      Zips2(zip, city, loc.mkString(","), pop, state)
    })
    val zipRDDDS = zipRDD.toDS();
    //    zipRDDDS.show()
    //
    //2、创建名为Zips的case class或者javaBean，用于将第一步创建的DF转换为DS
    //注意别忘记导入语句：import spark.implicits._
    val zipDS = zipDF.as[Zips]
    //3、显示DS中的数据
    //    zipDS.show(3)
    zipDS.createOrReplaceTempView("zips")

    //    //1、Spark Sql:以降序显示人口超过40000的 states, zip, cities,pop
    //    spark.sql("select state,city,zip, pop from zips where pop>40000 order by pop desc").show()
    //    //1、Spark Sql:以降序显示人口超过40000的 states, zip, cities,pop
    //    zipDS.select("state", "city", "zip", "pop").where("pop > 40000").orderBy(desc("pop")).show()
    //
    //    //2、显示名为CA的states中人口最多的三个城市
    //    spark.sql("select * from zips where state='CA' order by pop desc limit 3").show()
    //    zipDS.select("*").where("state=='CA'").orderBy(desc("pop")).limit(3).show()
    //
    //    //3、把所有州states的人口加起来，按降序排列,显示前10名
    //    spark.sql("select state,sum(pop) from zips group by state order by sum(pop) desc limit 10").show()
    //    zipDS.select("state", "pop").groupBy("state").sum("pop").orderBy(desc("sum(pop)")).limit(10).show()
    //    println("**************")
    //    /**
    //     * 聚合函数
    //     */
    //    //4、计算名为CA的state，每个city的zip总数、人口总量
    //    println("*****zipDS.select********")
    spark.sql("SELECT COUNT(zip), SUM(pop), city FROM zips WHERE state = 'CA' GROUP BY city ORDER BY SUM(pop) DESC").show(3)
    zipDS.select("zip", "pop", "city").filter('state === "CA").groupBy("city").agg(sum("pop"), count("zip"), min("pop"), max("pop")).orderBy(desc("sum(pop)")).show(3)
    zipDS.filter('state === "CA").select("zip", "pop", "city").groupBy("city").agg(sum("pop").alias("popsum"), count("zip").alias("zipcount")).orderBy(desc("popsum")).explain(true)

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
      * zipDS.select(col("city"),zipToLongUDF(col("zip")).as("zipToLong"),largerThanUDF(col("zip"),lit("99923")).alias("largerThan")).orderBy(desc("zipToLong")).show();
      */
    //    println("********zipToLong******")
    //    spark.udf.register("zipToLong", (z:String) => z.toLong)
    //    spark.udf.register("largerThan", (z:String,number:Long) => z.toLong>number)
    ////    spark.sql("SELECT city, zipToLong(zip) as zip_to_long FROM zips ORDER BY zip_to_long DESC").show()
    ////    spark.sql("SELECT city, zipToLong(zip) as zip_to_long,largerThan(zip,99923) as largerThan FROM zips ORDER BY zip_to_long DESC").show()
       val zipToLongUDF=udf((z:String) => z.toLong)
    ////    val largerThanUDF = udf((z: String, number: Long) => z.toLong>number)
    ////    println("********zipToLongUDF******")
    ////    zipDS.select(col("city"),zipToLongUDF(col("zip")).as("zipToLong"),largerThanUDF(col("zip"),lit("99923")).as("largerThan")).orderBy(desc("zipToLong")).show();
    //    /**
    //     * Spark On Hive:
    //     * Spark On Hive与Hive On Spark
    //     * Spark On Hive:
    //     * 1、是否需要启动hive?--不需要
    //     * 2、是否需要启动HDFS？---需要
    //     * 3、是否需要启动YARN？--不需要
    //     * 4、如何配置Spark On Hive？
    //     * 5、Spark On Hive的内部机制是什么？
    //     * 6、Spark On Hive实战：com.brave.prepare.SparkSqlHiveTest
    //     * 7、将作业数据保存到hive中
    //     */
    //    //5、把结果保存到hive中
        zipDS.write.mode(SaveMode.Overwrite).saveAsTable("hive_zips_table")
        spark.sql("show tables").show()
        spark.sql("select * from hive_zips_table").show(2)
    spark.close()
  }

  def zipToLong(z: String) = z.toLong

  def largerThan(z: String, number: Long) = z.toLong > number
}