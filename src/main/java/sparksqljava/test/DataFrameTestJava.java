//package sparksqljava.test;
//
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//
//import java.util.ArrayList;
//import java.util.List;
//
///**
// * Java 创建DataFrame的方式
// */
//public class DataFrameTestJava {
//    public static void main(String[] args) {
//
//        SparkSession spark = SparkSession.builder()
//                .appName("DataFrameTestJava")
//                .master("local")
//                .getOrCreate();
//
//        spark.sparkContext().setLogLevel("ERROR");
//        List<Male> males = new ArrayList<>();
//        males.add(new Male("张三", 26));
//        males.add(new Male("李四", 27));
//        males.add(new Male("赵五", 28));
//        males.add(new Male("王六", 26));
//
//        Dataset<Row> frame = spark.createDataFrame(males, Person.class);
//        frame.show();
//        frame.printSchema();
//
//        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
//        JavaRDD<Male> rdd = jsc.parallelize(males);
//        Dataset<Row> dataFrame = spark.createDataFrame(rdd, Male.class);
//        dataFrame.show();
//        dataFrame.printSchema();
//    }
//
//
//}
