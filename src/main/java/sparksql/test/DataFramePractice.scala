package sparksql.test

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * ataFrames操作详解
  */
object DataFramePractice {


  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().appName("DataFramePracticeJava").master("local").getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")
    val frame = ss.read.format("json").json("in/2015-summary.json")
    //
    //    rows(ss)
    //
    //    schema(ss, frame)

    //        columns(ss, frame)

    //    litS(ss, frame)

    //    addColumns(ss, frame)

    //    renameColumns(ss, frame)

    //    deleteColumns(ss, frame)

    //    changeType(ss, frame)

    //    filterRow(ss, frame)
    //    union(ss, frame)
    sort(ss, frame)
  }

  /**
    * 一行(Row)只是表示数据的一条记录。DataFrame中的每条数据记录必须是Row类型。
    * 我们可以从SQL、弹性分布式数据集(RDDs)、数据源或手动创建这些Rows
    *
    */
  private def rows(ss: SparkSession): Unit = {
    val frame = ss.range(10).toDF()
    frame.show()
    frame.printSchema()
    frame.collect().foreach(println)
  }

  /**
    * 一个schema就是一个StructType，由多个StructField类型的fields组成，
    * 每个field包括一个列名称、一个列类型、一个布尔型的标识（是否可以有缺失值和null值）
    */
  private def schema(ss: SparkSession, frame: DataFrame): Unit = {
    frame.printSchema()
    frame.show()
  }

  /**
    * columns操作
    *
    */
  private def columns(ss: SparkSession, frame: DataFrame): Unit = {
    col("someColumnName")
    column("someColumnName")

    frame.printSchema()

    frame.createOrReplaceTempView("flight")
    //    //api 方式
    //    frame.select("DEST_COUNTRY_NAME").show(2)
    //    //sql方式
    //    ss.sql("select * from flight where count>100 order by count").show()
    //
    //    //api查询多列
    //    frame.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show()


    //多种不同方式，交替使用
    //    frame.select(
    //      frame.col("DEST_COUNTRY_NAME"),
    //      col("DEST_COUNTRY_NAME"),
    //      column("DEST_COUNTRY_NAME"),
    //      expr("DEST_COUNTRY_NAME")
    //    ).show()

    //expr表达式的使用
    frame.select(expr("DEST_COUNTRY_NAME as destination")).show()
    //sql
    //    ss.sql("select DEST_COUNTRY_NAME as destination from flight limit 20").show()

    // 操作将列名更改为原来的名称
    frame.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME")).show()
  }

  /**
    * 字面常量转换为Spark类型（Literals）
    * 有时，我们需要将显式字面常量值传递给Spark，它只是一个值(而不是一个新列)。
    * 这可能是一个常数值或者我们以后需要比较的值。我们的方法是通过Literals，将给定编程语言的字面值转换为Spark能够理解的值:
    *
    */
  private def litS(ss: SparkSession, frame: DataFrame): Unit = {
    import org.apache.spark.sql.functions.{expr, lit}
    frame.select(expr("*"), lit(1).as("one")).show()
  }

  /**
    * 将新列添加到DataFrame中，这是通过在DataFrame上使用withColumn方法来实现的
    */
  private def addColumns(ss: SparkSession, frame: DataFrame): Unit = {
    frame.createOrReplaceTempView("flight")
    //让我们添加一个列，将数字1添加为一个列，列名为numberOne
    frame.withColumn("numberOne", lit(1)).show()
    //
    //    ss.sql("select *,1 as numberone from flight limit 2").show()

    //    frame.withColumn("withInCountry", expr("DEST_COUNTRY_NAME == ORIGIN_COUNTRY_NAME")).show()
    //withColumn函数有两个参数：列名和为DataFrame中的给定行创建值的表达式。我们也可以这样重命名列
    frame.withColumn("destination", expr("DEST_COUNTRY_NAME")).show()
  }

  /**
    * 重命名列
    */
  private def renameColumns(ss: SparkSession, frame: DataFrame): Unit = {
    frame.withColumnRenamed("DEST_COUNTRY_NAME", "destination").show(2)
  }

  /**
    * 删除列  drop
    */
  private def deleteColumns(ss: SparkSession, frame: DataFrame): Unit = {
    val frame1 = frame.withColumn("destination", expr("DEST_COUNTRY_NAME"))
    frame1.show(2)
    frame1.drop("destination").show(2)

    frame.drop("DEST_COUNTRY_NAME").show(2)
    frame.show(2)
  }

  /**
    * 更改类型
    * 验证：如果DF里面不存在count，那么程序将会报错
    */
  private def changeType(ss: SparkSession, frame: DataFrame): Unit = {
    frame.withColumn("count2", col("count").cast("long")).show(2)
    frame.createOrReplaceTempView("flight")
    ss.sql("select *,cast(count as long) as count1 from flight").show()
  }

  /**
    * 执行此操作有两种方法:您可以使用where或filter，它们都将执行相同的操作，并在使用DataFrames时接受相同的参数类型
    *
    * 您可能希望将多个过滤器放入相同的表达式中。尽管这是可能的，但它并不总是有用的，因为Spark会自动执行所有的过滤操作，
    * 而不考虑过滤器的排序。这意味着，如果您想指定多个和过滤器，只需将它们按顺序链接起来，让Spark处理其余部分:
    *
    */
  private def filterRow(ss: SparkSession, frame: DataFrame): Unit = {
    //sql  语句
    //SELECT * FROM flight WHERE count < 2 LIMIT 2
    frame.filter(col("count") < 20).show(2)

    frame.where(column("count") < 20).show(2)

    //SELECT * FROM flight WHERE count < 2 AND ORIGIN_COUNTRY_NAME != "Croatia" LIMIT 2
    frame.where(col("count") < 100).where(column("ORIGIN_COUNTRY_NAME") =!= "Croatia").show(2)


    //行去重
    //sql  SELECT COUNT(DISTINCT(ORIGIN_COUNTRY_NAME, DEST_COUNTRY_NAME)) FROM flight
    val l = frame.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()
    println(l)
  }

  /**
    * DataFrames是不可变的。这意味着用户不能向DataFrames追加，因为这会改变它。要附加到DataFrame，
    * 必须将原始的DataFrame与新的DataFrame结合起来。这只是连接了两个DataFramess。对于union two DataFrames，
    * 必须确保它们具有相同的模式和列数;否则，union将会失败
    */
  private def union(ss: SparkSession, frame: DataFrame): Unit = {
    val schema = frame.schema
    frame.printSchema()
    val newRows = Seq(
      Row("New Country", "Other Country", 5L),
      Row("New Country 2", "Other Country 3", 1L)
    )
    val newRdd = ss.sparkContext.parallelize(newRows)
    val dataFrame = ss.createDataFrame(newRdd, schema)
    frame.union(dataFrame).where("count=1")
      .where(col("ORIGIN_COUNTRY_NAME") =!= "United States")
      .show()
  }

  /**
    * 在对DataFrame中的值进行排序时，我们总是希望对DataFrame顶部的最大或最小值进行排序。
    * 有两个相同的操作可以执行这种操作：sort和orderBy。它们接受列表达式和字符串以及多个列。默认是按升序排序:
    *
    */
  private def sort(ss: SparkSession, frame: DataFrame): Unit = {
    //    frame.sort("count").show(5)
    //    frame.orderBy("count", "ORIGIN_COUNTRY_NAME").show(5)
    //    frame.orderBy(col("count"), col("ORIGIN_COUNTRY_NAME")).show(5)

    //要更明确地指定排序方向，需要在操作列时使用asc和desc函数
    //    frame.orderBy(expr("count desc")).show()
    //    frame.orderBy("count", "DEST_COUNTRY_NAME").show()
    //    frame.orderBy(desc("count"), asc("DEST_COUNTRY_NAME")).show()

    //优化目的，有时建议在另一组转换之前对每个分区进行排序。您可以使用sortWithinPartitions方法来执行以下操作:
    frame.sortWithinPartitions(expr("count desc")).show(30)
  }

}


