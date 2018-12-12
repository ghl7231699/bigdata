package sparksqljava.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * java 方式创建DataFrame
 */
public class DataFrameCreateJava {
    private static SparkSession session;

    public static void main(String[] args) {
        session = SparkSession.builder().master("local").appName("DataFrameCreateJava").getOrCreate();
        session.sparkContext().setLogLevel("ERROR");

        schemaToDataFrame();
    }

    /**
     * 使用List<Row>、schema创建DataFrame
     * 注意：创建StructField、StructType，要使用DataTypes的工厂方法。
     */
    private static void schemaToDataFrame() {
        System.out.println("*********schemaToDataFrame*********");
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create("张三", 28, "北京"));
        rows.add(RowFactory.create("李四", 29, "上海"));
        rows.add(RowFactory.create("王五", 27, "深圳"));
        rows.add(RowFactory.create("赵六", 28, "广州"));

        StructField[] field = new StructField[]{
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("age", DataTypes.IntegerType, false),
                DataTypes.createStructField("city", DataTypes.StringType, false),
        };

        StructType schema = DataTypes.createStructType(field);
        Dataset<Row> dataSet = session.createDataFrame(rows, schema);
        dataSet.show();
        dataSet.printSchema();

        dataSet.createOrReplaceTempView("person");
        session.sql("select name from person where age<29").show();
    }
}
