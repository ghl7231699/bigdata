package sparksqljava.teacher;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;

public class ZipProblemSolutionJava {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("ZipProblemSolution")
                .master("local[*]")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        //1、读取zips.json文件为DataFrame，并将列名_id重命名为zip
        Dataset<Row> zipDF = spark.read()
                .json("in/zips.json")
                .withColumnRenamed("_id", "zip");
        zipDF.javaRDD().map(new Function<Row, Row>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Row call(Row v1) throws Exception {
                return null;
            }
        });
        //2、创建名为Zips的case class或者javaBean，用于将第一步创建的DF转换为DS
        Encoder<Zips> zipsEncoder = Encoders.bean(Zips.class);
        Dataset<Zips> zipDS = zipDF.as(zipsEncoder);
        //3、显示DS中的数据
        zipDS.show();
        zipDS.createOrReplaceTempView("zips");
        //1、Spark Sql:以降序显示人口超过40000的 states, zip, cities,pop
        spark.sql("select state,city,zip, pop from zips where pop>4000 order by pop desc").show();
        zipDS.select("state", "city", "zip", "pop").filter("pop > 40000").orderBy(functions.desc("pop")).show();

        //2、显示名为CA的states中人口最多的三个城市
        spark.sql("select * from zips where state='CA' order by pop desc limit 3").show();
        zipDS.select("*").where("state=='CA'").orderBy(functions.desc("pop")).limit(3).show();

        //3、把所有州states的人口加起来，按降序排列,显示前10名
        spark.sql("select state,sum(pop) from zips group by state order by sum(pop) desc limit 10").show();
        zipDS.select("state", "pop").groupBy("state").sum("pop").orderBy(functions.desc("sum(pop)")).limit(10).show();
        spark.close();
    }

}
