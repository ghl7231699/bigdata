package sparksqljava.teacher;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

/**
 * SparkSQL大数据分析案例：天猫-商品列表(手机)数据分析
 *
 * @author Administrator
 */
public class PhoneSQL {

    public static void main(String[] args) {
        //实例化SparkSession(访问Spark的起点)
        /**
         * 分析数据的字段中文名称："店铺名称","产品名称","产品价格","产品销量","商品链接","产品评价"
         */
        SparkSession spark = SparkSession.builder().appName("phone sql")
                .master("local")
                .getOrCreate();

        spark.udf().register("splitCol", new UDF1<String, String>() {
            public String call(String s) throws Exception {
                return s.split("/")[0];
            }
        }, DataTypes.StringType);

        spark.udf().register("replaceChar", new UDF1<String, Float>() {
            public Float call(String s) throws Exception {
                return Float.valueOf(s.replaceAll("¥", ""));
            }
        }, DataTypes.FloatType);

        spark.udf().register("getNumberFromText", new UDF1<String, Float>() {
            public Float call(String s) throws Exception {
                if (s != null && !s.equals("")) {
                    String replaced = s.replaceAll("该款月成交 ", "").replaceAll("笔", "");
                    if (replaced != null && replaced.contains("万")) {
                        return Float.valueOf(replaced.replaceAll("万", "")) * 10000;
                    }

                    if (replaced != null) {
                        return Float.valueOf(replaced);
                    } else {
                        return 0f;
                    }
                } else {
                    return 0f;
                }
            }
        }, DataTypes.FloatType);

        Dataset<Row> df = spark.read().option("header", "true").csv("in/phone.csv");
        df.show();
        df.printSchema();


        df.createOrReplaceTempView("phone");
//		df.show();
//		spark.sql("select cpmc,splitCol(cpmc) as cpmc_new from phone").show();
        //最贵的10类手机
        spark.sql("select cpmc,cpjg,replaceChar(cpjg) as cpjg_new from phone order by cpjg_new desc").show(10);
        //销量最好的10类手机
        spark.sql("select cpmc,cpxl,getNumberFromText(cpxl) as cpxl_new from phone where cpxl is not null order by cpxl_new desc").show(10);
    }
}
