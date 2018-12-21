package sparksqljava.teacher;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class WordCountJavaSQL {

	public static void main(String[] args) {
		SparkSession spark=SparkSession.builder().master("local").appName("wordCount").getOrCreate();
		
		Dataset<String> file = spark.read().text("README.md").as(Encoders.STRING());
		Dataset<String> words=file.flatMap(new FlatMapFunction<String,String>() {
			@Override
			public Iterator<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" ")).iterator();
			}
		}, Encoders.STRING());
		
		//使用Dataset API做单词计数
		Dataset<Row> wordCount = words.groupBy("value").count();
		wordCount.orderBy(wordCount.col("count").desc()).show();
		
		
		
		//使用SQL语句做单词计数
//		words.createOrReplaceTempView("words");
//		spark.sql("select value, count(*) as total from words where value is not null group by value order by total desc").show();
		
	}

}
