package sparkcorejava.test.partition;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import sparkcorejava.test.rdd.JavaActionPractice;

import java.util.Arrays;
import java.util.List;

/**
 * rdd 分区 java
 */
public class RddPartitionJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(JavaActionPractice.class.getSimpleName()).setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");
        JavaRDD<String> file = sc.textFile("in/student.csv");
//
//        propertity(file);
//
//
//        System.out.println("defaultParallelism are " + sc.defaultParallelism());
//        System.out.println("defaultMinPartitions are " + sc.defaultMinPartitions());
        partitionerUsed(sc);

    }

    private static void propertity(JavaRDD<String> rdd) {
        if (rdd == null) {
            return;
        }
        System.out.println("partition size is " + rdd.partitions().size());
        System.out.println("partitioner are " + rdd.partitioner());
    }

    /**
     * 分区器的使用
     *
     * @param sc
     */
    private static void partitionerUsed(JavaSparkContext sc) {
        if (sc == null) {
            return;
        }
        Integer[] nums = new Integer[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        List<Integer> ints = Arrays.asList(nums);
        JavaRDD<Integer> parallelize = sc.parallelize(ints);
        System.out.println("NumPartitions:" + parallelize.getNumPartitions());
        System.out.println("partitioner:" + parallelize.partitioner());
        JavaPairRDD<Integer, Integer> map = parallelize.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2<>(integer, integer);
            }
        });
//        map.saveAsTextFile("out/hashPartition");
        map.partitionBy(new HashPartitioner(2)).saveAsTextFile("out/JavaHashPartition4");
    }
}
