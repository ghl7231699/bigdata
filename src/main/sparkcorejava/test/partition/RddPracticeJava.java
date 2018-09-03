package test.partition;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * RDD相关操作java版本
 */
public class RddPracticeJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RddPracticeJava").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        createRdd(sc);

        createRdd(sc, "in/国内航班数据说明.txt");
    }

    /**
     * 使用集合创建RDD
     *
     * @param sc
     */
    private static void createRdd(JavaSparkContext sc) {
        List<Integer> lists = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rdd = sc.parallelize(lists);

        System.out.println("RDD ----->" + rdd.toString());
    }

    /**
     * 从外部数据源创建RDD
     *
     * @param sc
     * @param path
     */
    private static void createRdd(JavaSparkContext sc, String path) {
        JavaRDD<String> datas = sc.textFile(path);
        System.out.println("RDD ----->" + datas.toString());
    }

}
