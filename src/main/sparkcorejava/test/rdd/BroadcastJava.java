package test.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 共享变量 广播 java版本
 */
public class BroadcastJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(PairRddPractice.class.getSimpleName()).setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");


        List<Tuple2<String, String>> tuple1s = Arrays.asList(new Tuple2<>("liyang", "kafka"), new Tuple2<>("lili", "hadoop"),
                new Tuple2<>("wangliu", "spark"), new Tuple2<>("zhaowu", "hbase"));

        List<Tuple2<String, Integer>> tuple2s = Arrays.asList(new Tuple2<>("liyang", 20), new Tuple2<>("lili", 21),
                new Tuple2<>("zhaosi", 22), new Tuple2<>("zhaowu", 23));


        JavaPairRDD<String, String> list1 = sc.parallelizePairs(tuple1s);
        JavaPairRDD<String, Integer> list2 = sc.parallelizePairs(tuple2s);

        List<Tuple2<String, String>> collect = list1.collect();
        Broadcast<List<Tuple2<String, String>>> broadcast = sc.broadcast(collect);

        list2.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Integer>>, Tuple3<String, String, String>>() {
            @Override
            public Iterator<Tuple3<String, String, String>> call(Iterator<Tuple2<String, Integer>> iterator) throws Exception {
                List<Tuple3<String, String, String>> list = new ArrayList<>();
                while (iterator.hasNext()) {
                    List<Tuple2<String, String>> value = broadcast.value();
                    Tuple2<String, Integer> next = iterator.next();
                    for (int i = 0; i < collect.size(); i++) {
                        String s = collect.get(i)._1;
                        if (next._1.equals(s)) {
                            list.add(new Tuple3<>(next._1, value.get(i)._2, String.valueOf(next._2)));
                        }
                    }

                }
                return list.iterator();
            }
        }).foreach(new VoidFunction<Tuple3<String, String, String>>() {
            @Override
            public void call(Tuple3<String, String, String> tuple3) throws Exception {
                System.out.println(tuple3._1() + ":\t" + tuple3._2() + "\t" + tuple3._3());
            }
        });
    }
}
