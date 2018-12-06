package sparkcorejava.homework;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.List;

/**
 * 1、航班数最多的航空公司，算出前6名
 * 2、北京飞往重庆的航空公司，有多少个？
 * java 版本
 */
public class FlightFilterJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(FlightFilterJava.class.getSimpleName()).setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");
        JavaRDD<String> file = sc.textFile("in/国内航班数据500条.csv");

//        maxAirPlane2(file);
        maxAirPlane3(file);
//        cityToCity(file, "北京", "重庆");
    }

    /**
     * 航班数最多的航空公司，输出前6
     * <p>
     * 没有进行去重，数据中可能会有重复数据
     *
     * @param rdd
     */
    private static void maxAirPlane1(JavaRDD<String> rdd) {

        JavaPairRDD<String, Integer> reduce = rdd.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String airName = s.split(",")[8];
                return new Tuple2<>(airName, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        JavaPairRDD<Integer, String> sort = reduce.map(new Function<Tuple2<String, Integer>, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> v1) throws Exception {
                return new Tuple2<>(v1._2, v1._1);
            }
        }).mapToPair(new PairFunction<Tuple2<Integer, String>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<Integer, String> tuple2) throws Exception {
                return tuple2;
            }
        }).sortByKey(false);

        System.out.println("航班数最多的航空公司为：" + sort.take(6));
    }

    /**
     * 取消重复航班，计算航空公司数量
     *
     * @param rdd
     */
    private static void maxAirPlane2(JavaRDD<String> rdd) {
        JavaPairRDD<String, Integer> reduce = rdd.mapToPair(new PairFunction<String, Tuple2<String, String>, Integer>() {
            @Override
            public Tuple2<Tuple2<String, String>, Integer> call(String v1) throws Exception {
                if ("".equals(v1)) {
                    return null;
                }
                String[] source = v1.split(",");
                return new Tuple2<>(new Tuple2<>(source[7], source[8]), 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Tuple2<String, String>, Integer> v1) throws Exception {
                return new Tuple2<>(v1._1()._2, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        JavaPairRDD<Integer, String> sort = reduce.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                return new Tuple2<>(tuple2._2, tuple2._1);
            }
        }).sortByKey(false);
        sort.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> tuple2) throws Exception {
                System.out.println(tuple2);
            }
        });
        List<Tuple2<Integer, String>> take = sort.take(6);
        for (int i = 0; i < take.size(); i++) {
            Tuple2<Integer, String> tuple2 = take.get(i);
            System.out.println("航班数最多的航空公司：" + (i + 1) + "：" + tuple2);
        }
    }

    private static void maxAirPlane3(JavaRDD<String> rdd) {
        if (rdd == null) {
            return;
        }
        //1、生成（航空公司+航班号，数量）去除重复数据
        JavaPairRDD<String, Integer> pair = rdd.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                if ("".equals(s)) {
                    return null;
                }
                String[] split = s.split(",");
                return new Tuple2<>((split[7] + "," + split[8]), 1);
            }
        });
        //2、（航空公司+航班号，数量)相同key值合并
        JavaPairRDD<String, Integer> reduce = pair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        reduce.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple2) throws Exception {
                return new Tuple2<>(tuple2._1.split(",")[1], 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                return new Tuple2<>(tuple2._2, tuple2._1);
            }
        }).sortByKey(false)
                .foreach(new VoidFunction<Tuple2<Integer, String>>() {
                    @Override
                    public void call(Tuple2<Integer, String> tuple2) throws Exception {
                        System.out.println(tuple2._2 + ":" + tuple2._1);
                    }
                });


    }

    /**
     * @param rdd
     * @param from 出发地
     * @param to   目的地
     */
    private static void cityToCity(JavaRDD<String> rdd, String from, String to) {
        if (rdd == null || from == null || to == null) {
            return;
        }

        rdd.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                String city1 = v1.split(",")[0];
                String city2 = v1.split(",")[3];
                return from.equals(city1) && to.equals(city2);
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple2<>(split[8], 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(tuple2);
            }
        });
    }
}
