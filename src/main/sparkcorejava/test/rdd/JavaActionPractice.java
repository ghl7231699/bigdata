package test.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * action 操作的java版本
 */
public class JavaActionPractice {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(JavaActionPractice.class.getSimpleName()).setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");
//        JavaRDD<String> file = sc.textFile("in/student.csv");
//        JavaRDD<String> file = sc.textFile("hdfs://aliyun:8020/user/input");
        JavaRDD<String> file = sc.textFile("hdfs://aliyun:8020/user/test1/");

        count(file);
//
//        take(file);

//        top(file);

//        countByValue(file);

//        reduce(file);

//        collect(sc);

//        groupBy(file);
    }

    private static void count(JavaRDD<String> file) {
        if (file == null) {
            return;
        }
        System.out.println("count : \t" + file.count());
    }

    /**
     * 从RDD返回n个元素。它试图减少它访问的分区数量，不能使用此方法来控制访问元素的顺序
     *
     * @param file
     */
    private static void take(JavaRDD<String> file) {
        System.out.println("take : \t" + file.take(5));
    }

    /**
     * 如果RDD中元素有序，那么可以使用top()从RDD中提取前几个元素
     *
     * @param file
     */
    private static void top(JavaRDD<String> file) {
        JavaRDD<Integer> map = file.map(new Function<String, Integer>() {
            @Override
            public Integer call(String v1) throws Exception {
                String[] split = v1.split(",");
                return Integer.valueOf(split[2]);
            }
        });
        System.out.println("top: \t" + map.top(3));
        map.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println("top map: \t" + integer);
            }
        });

        JavaRDD<String> map1 = file.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                String[] split = v1.split(",");
                return split[2] + "\t" + split[0] + "\t" + split[1];
            }
        });
        map1.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        System.out.println("exchange top: \t" + map1.top(8));
    }

    /**
     * countByValue()返回，每个元素都出现在RDD中的次数。例如：
     * RDD中的元素{1, 2, 2, 3, 4, 5, 5, 6} ,“rdd.countByValue()”{(1,1), (2,2), (3,1), (4,1),
     * (5,2), (6,1)}，返回一个hashmap (K, Int),包括每个key的计数。
     *
     * @param rdd
     */
    private static void countByValue(JavaRDD<String> rdd) {
        if (rdd == null) {
            return;
        }
        Map<String, Long> map = rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(",")).iterator();
            }
        }).countByValue();
        for (Map.Entry<String, Long> entry : map.entrySet()) {
            System.out.println("countByValue:\t" + entry.getKey() + " : " + entry.getValue());
        }
    }

    private static void reduce(JavaRDD<String> rdd) {
        if (rdd == null) {
            return;
        }
        String reduce = rdd.reduce(new Function2<String, String, String>() {
            @Override
            public String call(String v1, String v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println("reduce:\t" + reduce);
    }

    /**
     * collect()是将整个RDDs内容返回给driver程序的常见且最简单的操作。collect()的应用是单元测试，在单元测试中，期望整个RDD能够装入内存。
     * 如果使用了collect方法，但是driver内存不够，则内存溢出
     *
     * @param sc
     */
    private static void collect(JavaSparkContext sc) {
        JavaPairRDD<String, Tuple2<String, String>> join = PairRddPractice.join(sc);
        System.out.print(join.collect());
    }

    private static void groupBy(JavaRDD<String> rdd) {
        rdd.groupBy(new Function<String, Integer>() {
            @Override
            public Integer call(String v1) throws Exception {
                return Integer.valueOf(v1.split(",")[2]);
            }
        }).foreach(new VoidFunction<Tuple2<Integer, Iterable<String>>>() {
            @Override
            public void call(Tuple2<Integer, Iterable<String>> tuple2) throws Exception {
                System.out.print(tuple2);
            }
        });

    }
}
