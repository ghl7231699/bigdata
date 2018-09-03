package sparksql

import org.apache.spark.sql.SparkSession
import java.sql.Date
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.Row

/**
 * @author Administrator
 */
object DataFramePrepare {
  case class Person(name:String,age:Int,address:String,birthday:Date)
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
    .appName("DataFramePrepare")
    .master("local")
    .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    /**
     * import spark.implicits._
     * 导入隐式转换函数，可以将本地序列(seq), 列表或者RDD转为DataFrame
     * 可以使用toDF("name","age","address","birthday")指定列名
     * 如果不指定列名，spark默认设置列名为：_1,_2,_3,_4
     */
    val seq=Seq(
        ("zhangsan",10,"beijing",java.sql.Date.valueOf("2008-01-01")),
        ("lisi",20,"shanghai",java.sql.Date.valueOf("1998-01-01"))
        )
    val df=seq.toDF("name","age","address","birthday");
    df.show()
    df.printSchema()
    /**
     * 输出结果：
     * +--------+---+--------+----------+
      |    name|age| address|  birthday|
      +--------+---+--------+----------+
      |zhangsan| 10| beijing|2008-01-01|
      |    lisi| 20|shanghai|1998-01-01|
      +--------+---+--------+----------+
      
      root
       |-- name: string (nullable = true)
       |-- age: integer (nullable = false)
       |-- address: string (nullable = true)
       |-- birthday: date (nullable = true)
     */
    
    /**
     * 创建序列时，直接使用case class作为序列元素，则toDF()时，不需要指定列名。
     */
    println("*********case class seq*********")
    val personSeq=Seq(
        Person("zhangsan",10,"beijing",java.sql.Date.valueOf("2008-01-01")),
        Person("lisi",20,"shanghai",java.sql.Date.valueOf("1998-01-01"))
        )
        
    val personDF=personSeq.toDF();
    personDF.show()
    personDF.printSchema()
    /**
     * 输出结果：
     * +--------+---+--------+----------+
      |    name|age| address|  birthday|
      +--------+---+--------+----------+
      |zhangsan| 10| beijing|2008-01-01|
      |    lisi| 20|shanghai|1998-01-01|
      +--------+---+--------+----------+
      
      root
       |-- name: string (nullable = true)
       |-- age: integer (nullable = false)
       |-- address: string (nullable = true)
       |-- birthday: date (nullable = true)
     */
    
    /**
     * 通过隐式转换函数，可以将RDD转换为DataFrame
     */
    println("************RDD To DataFrame**************")
    val rdd=spark.sparkContext.parallelize(personSeq, 2)
    val rddToDF=rdd.toDF()
    rddToDF.show()
    rddToDF.printSchema()
    
    /**
     * 使用createDataFrame(rdd,schema)方法创建DataFrame
     */
    println("***********rddSchemaDF**************")
    val schema=StructType(List(
      StructField("name",StringType,nullable=false),    
      StructField("age",IntegerType,nullable=false),  
      StructField("address",StringType,nullable=false),  
      StructField("birthday",DateType,nullable=false)
    ))
    val rowRDD=spark.sparkContext.parallelize(Seq(
      Row("zhangsan",10,"beijing",java.sql.Date.valueOf("2008-01-01")),
      Row("lisi",20,"shanghai",java.sql.Date.valueOf("1998-01-01"))    
    ), 2)
    
    val rddSchemaDF=spark.createDataFrame(rowRDD, schema)
    rddSchemaDF.show()
    rddSchemaDF.printSchema()
    
    /**
     * 通过文件(json、csv、DB)创建DataFrame
     */
    println("***********fileDF**************")
    val fileDF = spark.read.json("people.json")
    fileDF.show()
    fileDF.printSchema()
    
    
    
  }
}