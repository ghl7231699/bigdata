package test.partition;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * pairRdd 练习
 */
public class PairRddPractice {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(PairRddPractice.class.getSimpleName()).setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

//        createPairRdd(sc);
//
        JavaRDD<String> rdd = sc.textFile("in/test.csv");
//        createPairRdd(rdd);
//        normalToPairRdd(sc);
        reduceByKey(rdd);
        combineBy(sc);

        sc.stop();
    }

    /**
     * 通过外部数据源
     *
     * @param rdd
     */
    private static void createPairRdd(JavaRDD<String> rdd) {
        rdd.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println("createPairRdd result is " + s);
            }
        });
    }

    /**
     * 使用集合创建RDD
     *
     * @param sc
     */
    private static void createPairRdd(JavaSparkContext sc) {
        List<Tuple2<String, Integer>> tuple2s = Arrays.asList(new Tuple2<>("liyang", 22), new Tuple2<>("lili", 21),
                new Tuple2<>("zhaosi", 22), new Tuple2<>("zhaowu", 22), new Tuple2<>("zhouyi", 23), new Tuple2<>("zhoubing", 22),
                new Tuple2<>("cuixinle", 22), new Tuple2<>("caiquanwu", 22), new Tuple2<>("wudajing", 22), new Tuple2<>("guli", 23), new Tuple2<>("guosan", 21));

        sc.parallelize(tuple2s).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t1) throws Exception {
                System.out.println("createPairRdd result is " + t1);
            }
        });
    }

    private static void normalToPairRdd(JavaSparkContext sc) {
        if (sc == null) {
            return;
        }
        List<String> list = Arrays.asList("liyang 200", "lili 30", "lili 21", "susi 25");

        JavaRDD<String> stringRdd = sc.parallelize(list);
        stringRdd.persist(StorageLevel.MEMORY_ONLY());
//        JavaPairRDD<String, Integer> pairRdd = stringRdd.mapToPair(new PairFunction<String, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(String s) throws Exception {
//                return new Tuple2<String, Integer>(s.split(" ")[0], Integer.valueOf(s.split(" ")[1]));
//            }
//        });
//
//        pairRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer v1, Integer v2) throws Exception {
//                return v1 + v2;
//            }
//        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> tuple2) throws Exception {
//                System.out.println("normalToPairRdd result is " + tuple2);
//            }
//        });

        stringRdd.mapToPair(new PairFunction<String, Integer, String>() {

            @Override
            public Tuple2<Integer, String> call(String s) throws Exception {
                return new Tuple2<Integer, String>(Integer.valueOf(s.split(" ")[1]), s.split(" ")[0]);
            }
        }).filter(new Function<Tuple2<Integer, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Integer, String> v1) throws Exception {
                return v1._1 > 20;
            }
        }).reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String v1, String v2) throws Exception {
                return v1;
            }
        }).foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                System.out.println(" result is " + integerStringTuple2);
            }
        });

    }

    private static void reduceByKey(JavaRDD<String> rdd) {
        if (rdd == null) {
            return;
        }
        rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] split = s.split(",");
                return Arrays.asList(split).iterator();
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(" reduceByKey:(" + tuple2._1 + ":\t" + tuple2._2 + ")");
            }
        });
    }

    private static void combineBy(JavaSparkContext sc) {
        if (sc == null) {
            return;
        }
        JavaRDD<String> rdd = sc.textFile("in/student.csv");
        //个人平均分
        //1 学生的学科数
//        rdd.map(new Function<String, Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> call(String v1) throws Exception {
//                String[] split = v1.split(",");
//                String replace = split[2].replace(" ", "");
//                return new Tuple2<String, Integer>(split[0], Integer.valueOf(replace));
//            }
//        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> tuple2) throws Exception {
//                System.out.println(" combineBy:(" + tuple2._1 + ":\t" + tuple2._2 + ")");
//            }
//        });
        //计算出每个学生几科成绩
//        map.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple2) throws Exception {
//                return new Tuple2<String, Integer>(tuple2._1, 1);
//            }
//        }).combineByKey(new Function<Integer, Object>() {
//        });
//        map.map(new Function<Tuple2<String,Integer>, Integer>() {
//            @Override
//            public Integer call(Tuple2<String, Integer> v1) throws Exception {
//                return v;
//            }
//        });

        JavaPairRDD<String, ScoreDetail> pair = rdd.map(new Function<String, ScoreDetail>() {
            @Override
            public ScoreDetail call(String v1) throws Exception {
                String[] split = v1.split(",");
                ScoreDetail detail = new ScoreDetail();
                detail.setName(split[0]);
                detail.setSubject(split[1]);
                detail.setScore(Integer.valueOf(split[2]));
                return detail;
            }
        }).mapToPair(new PairFunction<ScoreDetail, String, ScoreDetail>() {
            @Override
            public Tuple2<String, ScoreDetail> call(ScoreDetail scoreDetail) throws Exception {
                return new Tuple2<String, ScoreDetail>(scoreDetail.getName(), scoreDetail);
            }
        });
//        pair.map(new Function<Tuple2<Float, Integer>, Float>() {
//            @Override
//            public Float call(Tuple2<Float, Integer> v1) throws Exception {
//                return v1._1 / v1._2;
//            }
//        }).foreach(new VoidFunction<Float>() {
//            @Override
//            public void call(Float aFloat) throws Exception {
//                System.out.println(" average is :" + aFloat);
//            }
//        });


        //1、创建createCombiner：组合器函数，输入参数为RDD[K,V]中的V（即ScoreDetail对象），输出为tuple2(学生成绩，1)
        Function<ScoreDetail, Tuple2<Float, Integer>> createCombiner = new Function<ScoreDetail, Tuple2<Float, Integer>>() {
            @Override
            public Tuple2<Float, Integer> call(ScoreDetail v1) throws Exception {
                return new Tuple2<Float, Integer>((float) v1.score, 1);
            }
        };

        //2、mergeValue：合并值函数，输入参数为(C,V)即（tuple2（学生成绩，1），ScoreDetail对象），输出为tuple2（学生成绩，2）
        Function2<Tuple2<Float, Integer>, ScoreDetail, Tuple2<Float, Integer>> mergeValue = new Function2<Tuple2<Float, Integer>, ScoreDetail, Tuple2<Float, Integer>>() {
            @Override
            public Tuple2<Float, Integer> call(Tuple2<Float, Integer> v1, ScoreDetail v2) throws Exception {
                return new Tuple2<Float, Integer>(v1._1() + v2.score, v1._2() + 1);
            }
        };

        //3、mergeCombiners：合并组合器函数，对多个节点上的数据合并，输入参数为(C,C)，输出为C

        Function2<Tuple2<Float, Integer>, Tuple2<Float, Integer>, Tuple2<Float, Integer>> mergeCombiners = new Function2<Tuple2<Float, Integer>, Tuple2<Float, Integer>, Tuple2<Float, Integer>>() {
            @Override
            public Tuple2<Float, Integer> call(Tuple2<Float, Integer> v1, Tuple2<Float, Integer> v2) throws Exception {
                return new Tuple2<Float, Integer>(v1._1() + v2._1(), v1._2() + v2._2());
            }
        };

        //4、combineByKey并求均值
        JavaPairRDD<String, Float> res = pair.combineByKey(createCombiner, mergeValue, mergeCombiners, 2)
                .mapToPair(x -> new Tuple2<String, Float>(x._1(), x._2()._1() / x._2()._2()));
        res.foreach(new VoidFunction<Tuple2<String, Float>>() {
            @Override
            public void call(Tuple2<String, Float> tuple2) throws Exception {
                System.out.println(" average:(" + tuple2._1 + ":\t" + tuple2._2 + ")");
            }
        });

    }

    public static class ScoreDetail implements Serializable {
        String name;
        String subject;
        int score;

        public ScoreDetail() {
        }

        public ScoreDetail(String name, String subject, int score) {
            this.name = name;
            this.subject = subject;
            this.score = score;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getSubject() {
            return subject;
        }

        public void setSubject(String subject) {
            this.subject = subject;
        }

        public int getScore() {
            return score;
        }

        public void setScore(int score) {
            this.score = score;
        }
    }

}
