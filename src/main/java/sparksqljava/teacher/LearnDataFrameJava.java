package sparksqljava.teacher;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class LearnDataFrameJava {
    public static void main(String[] args){
        SparkSession spark=SparkSession.builder()
                .master("local")
                .appName("dataFramePrepare")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        List<Person> data = new ArrayList<Person>();
        data.add(new Person("张三",new Long(30),"北京"));
        data.add(new Person("李四",new Long(28),"上海"));

        Dataset<Row> listBeanDF = spark.createDataFrame(data, Person.class);
//        listBeanDF.show();
//        listBeanDF.printSchema();

//        JavaSparkContext jsc=new JavaSparkContext(spark.sparkContext());
//        JavaRDD<Person> personRDD = jsc.parallelize(data);
//        Dataset<Row> rddBeanDF=spark.createDataFrame(personRDD, Person.class);
//        rddBeanDF.show();
//        rddBeanDF.printSchema();

//        listBeanDF.createOrReplaceTempView("person");
//        Dataset<Row> sql = spark.sql("select * from person");
//        sql.show();

        List<Row> rows=new ArrayList<Row>();
        rows.add(RowFactory.create("zhangsan",20,"shanghai"));
        rows.add(RowFactory.create("lisi",28,"beijing"));

        StructField[] fields=new StructField[]{
                DataTypes.createStructField("name",DataTypes.StringType,false),
                DataTypes.createStructField("age",DataTypes.IntegerType,false),
                DataTypes.createStructField("address",DataTypes.StringType,false)
        };
        StructType schema= DataTypes.createStructType(fields);
        Dataset<Row> schemaDF = spark.createDataFrame(rows, schema);
//        schemaDF.show();

        JavaSparkContext jsc=new JavaSparkContext(spark.sparkContext());
        JavaRDD<Row> rddRows = jsc.parallelize(rows);
        Dataset<Row> rddDF = spark.createDataFrame(rddRows, schema);
        rddDF.show();
        rddDF.printSchema();

        //DF转DS 只需一个Encoder
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> personDataset = rddDF.as(personEncoder);

        rddDF.foreach(new ForeachFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                //untyped
//               Long.valueOf(row.getAs("hight"));
            }
        });

        personDataset.foreach(new ForeachFunction<Person>() {
            @Override
            public void call(Person person) throws Exception {
                //typed
//                person.getHight();
            }
        });

    }
}
