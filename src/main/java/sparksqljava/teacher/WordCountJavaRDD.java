package sparksqljava.teacher;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCountJavaRDD {

	public static void main(String[] args) {
		SparkConf conf=new SparkConf();
		conf.setMaster("local");
		conf.setAppName("wordCountJava");
		
		JavaSparkContext jsc=new JavaSparkContext(conf);
		JavaRDD<String> fileRDD=jsc.textFile("README.md");
		JavaRDD<String> words=fileRDD.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" ")).iterator();
			}
		});
		
		JavaPairRDD<String, Integer> wordPairs=words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		
		JavaPairRDD<String, Integer> wordCount=wordPairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		
		JavaPairRDD<Integer, String> wordCount2=wordCount.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {
			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> t2)throws Exception {
				return new Tuple2<Integer, String>(t2._2, t2._1);
			}
		});
		wordCount.collect();
		List<Tuple2<Integer,String>> result=wordCount2.sortByKey(false).take(10);
		for(Tuple2<Integer,String> tuple2 : result){
			System.out.println(tuple2._1+" : "+tuple2._2);
		}
		
//		textFile  flatMap  mapToPair  reduceByKey  collect  sortByKey  take
	}

}
