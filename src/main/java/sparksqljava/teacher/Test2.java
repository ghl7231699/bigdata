package sparksqljava.teacher;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

public class Test2 {
    public static  void main(String[] args){
        SparkConf sparkConf=new SparkConf().setAppName("first").setMaster("local");
        JavaSparkContext javaSparkContext=new JavaSparkContext(sparkConf);
        List<Integer> data=Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> distData=javaSparkContext.parallelize(data);
        int count=distData.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        System.out.println("count== "+count);
    }
}
