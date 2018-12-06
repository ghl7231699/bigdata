package sparkcorejava.test.rdd;

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
//        JavaRDD<String> rdd = sc.textFile("in/test.csv");
        JavaRDD<String> rdd = sc.textFile("in/student.csv");
//        createPairRdd(rdd);
//        normalToPairRdd(sc);
        reduceByKey(rdd);
//        average(sc);
//        combineByKey(sc);
//        averageScore(rdd);

//        join(sc);

//        sc.stop();
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
        }).sortByKey()
                .foreach(new VoidFunction<Tuple2<String, Integer>>() {
                    @Override
                    public void call(Tuple2<String, Integer> tuple2) throws Exception {
                        System.out.println(" reduceByKey:(" + tuple2._1 + ":\t" + tuple2._2 + ")");
                    }
                });
    }

    private static void averageScore(JavaRDD<String> rdd) {
        if (rdd == null) {
            return;
        }

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
        JavaPairRDD<String, ScoreDetail> reduce = pair.reduceByKey(new Function2<ScoreDetail, ScoreDetail, ScoreDetail>() {
            @Override
            public ScoreDetail call(ScoreDetail v1, ScoreDetail v2) throws Exception {
                return new ScoreDetail(v1.getName(), v1.getSubject() + "," + v2.getSubject(), v1.getScore() + v2.getScore());
            }
        });
        reduce.foreach(new VoidFunction<Tuple2<String, ScoreDetail>>() {
            @Override
            public void call(Tuple2<String, ScoreDetail> t) throws Exception {
                System.out.println(t._1 + ": " + t._2.getScore() / t._2.getSubject().split(",").length);
            }
        });
    }

    public static JavaPairRDD<String, Tuple2<String, String>> join(JavaSparkContext sc) {
        if (sc == null) {
            return null;
        }
        JavaRDD<String> rdd1 = sc.textFile("in/test.csv");

        JavaPairRDD<String, String> fire = rdd1.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                return v1.contains("FIRE");
            }
        }).mapToPair((PairFunction<String, String, String>) s -> {
            String[] split = s.split(",");
            return new Tuple2<>(split[0], split[3]);
        });

        JavaPairRDD<String, String> lieutenant = rdd1.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                return v1.contains("LIEUTENANT");
            }
        }).mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple2<>(split[0], split[2]);
            }
        });

        JavaPairRDD<String, Tuple2<String, String>> join = fire.join(lieutenant);
//        join.sortByKey().foreach(new VoidFunction<Tuple2<String, Tuple2<String, String>>>() {
//            @Override
//            public void call(Tuple2<String, Tuple2<String, String>> tuple2) throws Exception {
//                System.out.println(" join result is : " + tuple2);
//            }
//        });
        return join;
    }

    private static void average(JavaSparkContext sc) {
        if (sc == null) {
            return;
        }
        List<Tuple2<String, Double>> tuple2s = Arrays.asList(new Tuple2<>("Fred", 88.0), new Tuple2<>("Fred", 95.0), new Tuple2<>("Fred", 91.0),
                new Tuple2<>("Wilma", 93.0), new Tuple2<>("Wilma", 95.0), new Tuple2<>("Wilma", 98.0));

        //rdd(学生，成绩)
        JavaPairRDD<String, Double> rdd = sc.parallelizePairs(tuple2s);

        //Function(V,C)（成绩，科目数）
        //例如 此时返回的是（88.0，1） 成绩是88 科目数为1
        Function<Double, Tuple2<Double, Integer>> createCombiner = new Function<Double, Tuple2<Double, Integer>>() {

            @Override
            public Tuple2<Double, Integer> call(Double v1) throws Exception {
                return new Tuple2<>(v1, 1);
            }
        };
        // //Function2(C,V,C)（成绩，（成绩，科目数））
        /**
         * 这里的v1  得到的就是（88.0，1）在同一个分区内 我们又碰到了一个Fred 所以需要将之前的分数+现在的这个分数：v1._1 + v2,科目数量+1 即 v1._2 + 1
         */
        Function2<Tuple2<Double, Integer>, Double, Tuple2<Double, Integer>> mergeValue = new Function2<Tuple2<Double, Integer>, Double, Tuple2<Double, Integer>>() {

            @Override
            public Tuple2<Double, Integer> call(Tuple2<Double, Integer> v1, Double v2) throws Exception {
                return new Tuple2<Double, Integer>(v1._1 + v2, v1._2 + 1);
            }
        };
        /**
         * Fred 的数据可能分布在多个分区，因此需要进行合并，及多个分区的同个人的成绩相加：v1._1 + v2._1，科目相加 v1._2 + v2._2
         */
        Function2<Tuple2<Double, Integer>, Tuple2<Double, Integer>, Tuple2<Double, Integer>> mergeCombiners = new Function2<Tuple2<Double, Integer>, Tuple2<Double, Integer>, Tuple2<Double, Integer>>() {

            @Override
            public Tuple2<Double, Integer> call(Tuple2<Double, Integer> v1, Tuple2<Double, Integer> v2) throws Exception {
                return new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2);
            }
        };
        //聚合处理 (学生,(总分，总科目))
        rdd.combineByKey(createCombiner, mergeValue, mergeCombiners).map(new Function<Tuple2<String, Tuple2<Double, Integer>>, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<String, Tuple2<Double, Integer>> v1) throws Exception {
                return new Tuple2<String, Double>(v1._1, v1._2._1 / v1._2._2);
            }
        }).foreach(new VoidFunction<Tuple2<String, Double>>() {
            @Override
            public void call(Tuple2<String, Double> aDouble) throws Exception {
                System.out.println(aDouble._1 + ":\t" + aDouble._2());
            }
        });

    }

    /**
     * createCombiner: V => C ，这个函数把当前的值作为参数，此时我们可以对其做些附加操作(类型转换)并把它返回 (这一步类似于初始化操作)
     * mergeValue: (C, V) => C，该函数把元素V合并到之前的元素C(createCombiner)上 (这个操作在每个分区内进行)
     * mergeCombiners: (C, C) => C，该函数把2个元素C合并 (这个操作在不同分区间进行)
     * <p>
     * combineByKey(Function(V,C),Function2(C,V,C),Function2(C,C,C)）
     *
     * @param sc
     */
    private static void combineByKey(JavaSparkContext sc) {
        if (sc == null) {
            return;
        }
        JavaRDD<String> rdd = sc.textFile("in/student.csv");

        JavaRDD<ScoreDetail> student = rdd.map(new Function<String, ScoreDetail>() {
            @Override
            public ScoreDetail call(String v1) throws Exception {
                String[] split = v1.split(",");
                ScoreDetail detail = new ScoreDetail();
                detail.setName(split[0]);
                detail.setSubject(split[1]);
                detail.setScore(Integer.valueOf(split[2]));
                return detail;
            }
        });
        JavaPairRDD<String, ScoreDetail> pair = student.mapToPair(new PairFunction<ScoreDetail, String, ScoreDetail>() {
            @Override
            public Tuple2<String, ScoreDetail> call(ScoreDetail scoreDetail) throws Exception {
                return new Tuple2<String, ScoreDetail>(scoreDetail.getName(), scoreDetail);
            }
        });

        JavaPairRDD<String, ScoreDetail> subject = student.mapToPair(new PairFunction<ScoreDetail, String, ScoreDetail>() {
            @Override
            public Tuple2<String, ScoreDetail> call(ScoreDetail scoreDetail) throws Exception {
                return new Tuple2<String, ScoreDetail>(scoreDetail.getSubject(), scoreDetail);
            }
        });

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
                System.out.println(" student average:(" + tuple2._1 + ":\t" + tuple2._2 + ")");
            }
        });
        subject.combineByKey(createCombiner, mergeValue, mergeCombiners, 2)
                .mapToPair(new PairFunction<Tuple2<String, Tuple2<Float, Integer>>, String, Float>() {
                    @Override
                    public Tuple2<String, Float> call(Tuple2<String, Tuple2<Float, Integer>> tuple2) throws Exception {
                        return new Tuple2<String, Float>(tuple2._1, tuple2._2._1 / tuple2._2._2);
                    }
                })
                .foreach(new VoidFunction<Tuple2<String, Float>>() {
                    @Override
                    public void call(Tuple2<String, Float> tuple2) throws Exception {
                        System.out.println(" subject average:(" + tuple2._1 + ":\t" + tuple2._2 + ")");
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

        @Override
        public String toString() {
            return super.toString();
        }
    }

}
