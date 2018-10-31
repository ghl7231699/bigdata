package test.partition;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * RDD相关操作java版本
 */
public class RddPracticeJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RddPracticeJava").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        JavaRDD<String> file = sc.textFile("in/test.csv");

//        createRdd(sc);
//
//        createRdd(sc, "in/国内航班数据说明.txt");
//        map(file);
//        flatMap(file);
//        filter(file);
//        mapPartitions(file);
        mapPartitionWithIndex(sc, file);
        union(sc);
        intersection(sc);
    }

    /**
     * 使用集合创建RDD
     * R
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

    private static void map(JavaRDD<String> rdd) {
        if (rdd == null) {
            return;
        }
        rdd.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                return v1.toLowerCase();
            }
        }).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.print("result is  " + s + "\n");
            }
        });
    }

    private static void flatMap(JavaRDD<String> rdd) {
        if (rdd == null) {
            return;
        }
        rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] split = s.split(",");
                return Arrays.asList(split).iterator();
            }
        }).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.print("flatMap result is  " + s + "\n");
            }
        });
    }

    private static void filter(JavaRDD<String> rdd) {
        if (rdd == null) {
            return;
        }
        rdd.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                return v1.contains("Salary");
            }
        }).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println("filter datas are " + s);
            }
        });
    }

    private static void mapPartitions(JavaRDD<String> rdd) {
        if (rdd == null) {
            return;
        }
        rdd.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {//第二个string 参数为输出的格式
            @Override
            public Iterator<String> call(Iterator<String> iterator) throws Exception {
                List<String> list = new ArrayList<>();
                while (iterator.hasNext()) {
                    String next = iterator.next();
                    if (next.contains("Salary")) {
                        list.add(next);
                    }
                }
                return list.iterator();
            }
        }).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println("mapPartitions datas are " + s);
            }
        });
    }

    private static void mapPartitionWithIndex(JavaSparkContext sc, JavaRDD<String> rdd) {
        if (rdd == null) {
            return;
        }
        rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<Tuple2<Integer, String>>>() {
            //参数：integer：分区值 Iterator<String>输入类型 Iterator<Tuple2<Integer, String>>：输出类型
            @Override
            public Iterator<Tuple2<Integer, String>> call(Integer v1, Iterator<String> v2) throws Exception {
                List<Tuple2<Integer, String>> tuple2 = new ArrayList<>();
                while (v2.hasNext()) {
                    String next = v2.next();
                    if (next.contains("FIRE")) {
                        tuple2.add(new Tuple2<>(v1, next));
                    }
                }
                return tuple2.iterator();
            }
        }, false)
                .foreach(new VoidFunction<Tuple2<Integer, String>>() {
                    @Override
                    public void call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                        System.out.println("mapPartitionWithIndex datas:  index== " + integerStringTuple2._1 + "\t value is " + integerStringTuple2._2);
                    }
                });

        JavaRDD<Integer> source = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2);
        JavaRDD<Tuple2<Integer, Integer>> tuple2JavaRDD = source.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<Tuple2<Integer, Integer>>>() {
            @Override
            public Iterator<Tuple2<Integer, Integer>> call(Integer partIndex, Iterator<Integer> it) throws Exception {
                ArrayList<Tuple2<Integer, Integer>> tuple2s = new ArrayList<>();
                while (it.hasNext()) {
                    int next = it.next();
                    tuple2s.add(new Tuple2<>(partIndex, next));
                }
                return tuple2s.iterator();
            }
        }, false);

        tuple2JavaRDD.foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void call(Tuple2<Integer, Integer> tp2) throws Exception {
                System.out.println(tp2);
            }
        });
    }

    private static void union(JavaSparkContext sc) {
        if (sc == null) {
            return;
        }

        List<String> d1 = Arrays.asList("spark", "hadoop", "zookeeper");
        List<String> d2 = Arrays.asList("hive", "hbase", "kafka", "sqoop");

        JavaRDD<String> r1 = sc.parallelize(d1);
        JavaRDD<String> r2 = sc.parallelize(d2);
        r1.union(r2).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println("union datas are  " + s);
            }
        });
    }

    private static void intersection(JavaSparkContext sc) {
        List<String> d1 = Arrays.asList("spark", "hadoop", "zookeeper", "sqoop");
        List<String> d2 = Arrays.asList("hive", "hbase", "kafka", "sqoop");

        JavaRDD<String> r1 = sc.parallelize(d1);
        JavaRDD<String> r2 = sc.parallelize(d2);

        r1.intersection(r2).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                if ("".equals(s)) {
                    System.out.println("intersection datas are  ");
                } else {
                    System.out.println("intersection datas are  " + s);
                }
            }
        });
    }
}
