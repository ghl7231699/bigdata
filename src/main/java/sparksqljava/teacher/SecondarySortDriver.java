package sparksqljava.teacher;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * Spark  简单二次排序
 * @author Brave
 *
 */
public class SecondarySortDriver {

	public static void main(String[] args) {
		SparkConf conf=new SparkConf();
		conf.setAppName("secondary sort");
		conf.setMaster("local");
		JavaSparkContext jsc=new JavaSparkContext(conf);
		JavaRDD<String> personRDD=jsc.textFile("names.csv");
		JavaPairRDD<String,String> pairRDD=personRDD.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String line) throws Exception {
				String[] nameFields=line.split(",");
				return new Tuple2<String, String>(nameFields[0], nameFields[1]);
			}
		});
		
		JavaPairRDD<String, Iterable<String>> listRDD = pairRDD.groupByKey().mapValues(new Function<Iterable<String>, Iterable<String>>() {
			@Override
			public Iterable<String> call(Iterable<String> iter)throws Exception {
				List list=IteratorUtils.toList(iter.iterator());
				list.sort(new StringComparator());
				return list;
			}
		});
		
		 JavaRDD<Tuple2<String, String>> resultRDD = listRDD.sortByKey().flatMap(new FlatMapFunction<Tuple2<String,Iterable<String>>, Tuple2<String,String>>() {
			@Override
			public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> t2) throws Exception {
				List<Tuple2<String,String>> list=new ArrayList<Tuple2<String,String>>();
				String key=t2._1;
				Iterable<String> iter = t2._2;
				for(String str : iter){
					list.add(new Tuple2(key,str));
				}
				return list.iterator();
			}
		});
		
		 resultRDD.foreach(new VoidFunction<Tuple2<String,String>>() {
			@Override
			public void call(Tuple2<String, String> t2) throws Exception {
				System.out.println(t2._1+","+t2._2);
			}
		});

	}

}
