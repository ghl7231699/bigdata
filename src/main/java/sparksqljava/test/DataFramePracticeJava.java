package sparksqljava.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * DataFrames操作java版
 */
public class DataFramePracticeJava {
    private static SparkSession session;

    public static void main(String[] args) {
        session = SparkSession.builder().master("local").appName("DataFrameCreateJava").getOrCreate();
        session.sparkContext().setLogLevel("ERROR");

        rows();
    }

    /**
     *
     */
    private static void rows() {
        Dataset<Row> rowDataset = session.range(2).toDF();
        rowDataset.show();
        rowDataset.printSchema();
    }
}
