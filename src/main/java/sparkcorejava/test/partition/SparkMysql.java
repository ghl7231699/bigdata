package sparkcorejava.test.partition;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import sparkcorejava.test.rdd.JavaActionPractice;
import utils.JdbcUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Iterator;

/**
 * spark数据存入mysql
 */
public class SparkMysql {
    private static String sql = "insert into flight(airline,number) values (?,?)";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(JavaActionPractice.class.getSimpleName()).setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");
        JavaRDD<String> file = sc.textFile("in/student.csv");


        JavaPairRDD<String, String> pair = file.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                if ("".equals(s)) {
                    return null;
                }
                String[] split = s.split(",");
                return new Tuple2<>(split[1], split[2]);
            }
        });
        pair.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> iterator) throws Exception {
                Connection connection = JdbcUtils.getConnection();
                PreparedStatement preparedStatement = null;
                while (iterator.hasNext()) {

                    Tuple2<String, String> next = iterator.next();
                    if (next != null) {
                        preparedStatement = connection.prepareStatement(sql);
                        preparedStatement.setString(1, next._1);
                        preparedStatement.setString(2, next._2);
                        preparedStatement.executeUpdate();
                    }
                }
                JdbcUtils.free(preparedStatement, connection);
            }
        });

    }

    private static void insertMysql() {

    }
}
