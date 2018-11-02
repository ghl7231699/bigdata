package test.partition;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import test.rdd.JavaActionPractice;

/**
 * rdd 分区 java
 */
public class RddPartitionJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(JavaActionPractice.class.getSimpleName()).setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");
        JavaRDD<String> file = sc.textFile("in/student.csv");

        propertity(file);

    }

    private static void propertity(JavaRDD<String> rdd) {
        System.out.println("partition size is " + rdd.partitions().size());
        System.out.println("partitioner are " + rdd.partitioner().get());

    }
}
