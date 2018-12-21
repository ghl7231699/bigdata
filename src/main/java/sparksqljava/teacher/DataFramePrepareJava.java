package sparksqljava.teacher;


import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Function1;

public class DataFramePrepareJava {

	public static void main(String[] args) {
		SparkSession spark=SparkSession.builder()
				.master("local")
				.appName("dataFramePrepare")
				.getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		List<Person> data = new ArrayList<Person>();
	    data.add(new Person("张三",new Long(30),"北京"));
	    data.add(new Person("李四",new Long(28),"上海"));
	    /**
	     * 使用List、JavaBean创建DataFrame
	     * 注意： createDataFrame(data: java.util.List[_], beanClass: Class[_])的第二个方法参数
	     * beanClass，是指JavaBean.class.不能使用String.class
	     * 否则报错：
	     * org.apache.spark.sql.types.StringType$ cannot be cast to org.apache.spark.sql.types.StructType
	     */
		Dataset<Row> listBeanDF = spark.createDataFrame(data, Person.class);
		listBeanDF.show();
		listBeanDF.printSchema();
		
		/**
	     * 使用RDD、JavaBean创建DataFrame
	     * 注意： createDataFrame(rdd: JavaRDD[_], beanClass: Class[_])的第二个方法参数
	     * beanClass，是指JavaBean.class.不能使用String.class
	     * 否则报错：
	     * org.apache.spark.sql.types.StringType$ cannot be cast to org.apache.spark.sql.types.StructType
	     */
		JavaSparkContext jsc=new JavaSparkContext(spark.sparkContext());
		JavaRDD<Person> personRDD = jsc.parallelize(data);
		Dataset<Row> rddBeanDF=spark.createDataFrame(personRDD, Person.class);
		rddBeanDF.show();
		rddBeanDF.printSchema();
		
		/**
	     * 使用List<Row>、schema创建DataFrame
	     * 注意：创建StructField、StructType，要使用DataTypes的工厂方法。
	     */
		List<Row> rows=new ArrayList<Row>();
		rows.add(RowFactory.create("zhangsan",20,"beijing"));
		rows.add(RowFactory.create("lisi",18,"shanghai"));
		StructField[] fields=new StructField[]{
				DataTypes.createStructField("name", DataTypes.StringType, false),
				DataTypes.createStructField("age", DataTypes.IntegerType, false),
				DataTypes.createStructField("address", DataTypes.StringType, false)
		};
		StructType schema=DataTypes.createStructType(fields);
		Dataset<Row> listSchemaDF=spark.createDataFrame(rows, schema);
		listSchemaDF.show();
		listSchemaDF.printSchema();
		
		/**
	     * 使用RDD<Row>、schema创建DataFrame
	     * 注意：创建StructField、StructType，要使用DataTypes的工厂方法。
	     */
		System.out.println("*******rddSchemaDF********");
		JavaRDD<Row> rddRows = jsc.parallelize(rows);
		Dataset<Row> rddSchemaDF=spark.createDataFrame(rddRows, schema);
		rddSchemaDF.show();
		rddSchemaDF.printSchema();
		
		System.out.println("********listEncoderDS***************");
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Dataset<Person> listEncoderDS = spark.createDataset(data, personEncoder);
		listEncoderDS.show();
		listEncoderDS.printSchema();
		/**
		 * 通过读取文件的方式创建DataFrame
		 */
		System.out.println("************fileDF************");
		Dataset<Row> fileDF=spark.read().json("people.json");
		fileDF.show();
		fileDF.printSchema();
		
		/**
		 * 通过读取文件的方式创建Dataset
		 */
		System.out.println("***************fileDS**************");
		Dataset<Person> fileDS=spark.read().json("people.json").as(personEncoder);
		fileDS.show();
		fileDS.printSchema();
		
		/**
		 * 通过spark.sql方法创建DF
		 */
		System.out.println("****************sqlDF***************");
		fileDF.createOrReplaceTempView("people");
		Dataset<Row> sqlDF = spark.sql("select * from people");
		sqlDF.show();
		sqlDF.printSchema();
		
		/**
		 * 获取Dataset和DataFrame中的数据，体会两者之间的区别：强类型提供了编译时异常!!!
		 */
		System.out.println("**********Dataset和DataFrame中的数据*************");
		Dataset<String> nameDS = fileDF.map(new MapFunction<Row,String>() {
			@Override
			public String call(Row row) throws Exception {
				return "DFName: "+row.getAs("name");
			}
		}, Encoders.STRING());
		nameDS.show();
		nameDS.printSchema();
		
		Dataset<String> nameDS2 = fileDS.map(new MapFunction<Person,String>() {
			@Override
			public String call(Person person) throws Exception {
				return "DSName: "+person.getName();
			}
		}, Encoders.STRING());
		
		nameDS2.show();
		nameDS2.printSchema();
	}
}

