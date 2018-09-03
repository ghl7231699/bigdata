package test.teacher;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Spark 复杂的二次排序
 * 对应数据源：employee.csv
 * 数据字段说明：
 * First Name 名
 * Last Name  姓
 * Job Titles 职称
 * Department 部门
 * Full or Part-Time 全职或兼职
 * Salary 工资
 * Typical Hours 小时工
 * Annual Salary 年薪
 * Hourly Rate  时薪
 * 根据Department字段对数据进行分组，然后我们将首先根据Annual Salary排序，然后根据firstName排序。
 *
 * @author Brave
 * var rdd1 = sc.makeRDD(Seq(10, 6, 2, 16, 9))
 */
public class SecondarySortDriver2 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("pattern").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = jsc.textFile("employee.csv");
        jsc.setLogLevel("ERROR");
        JavaPairRDD<Employee_Key, Employee_Value> pair = rdd.mapToPair(new PairFunction<String, Employee_Key, Employee_Value>() {
            @Override
            public Tuple2<Employee_Key, Employee_Value> call(String data) throws Exception {
                String[] field = data.split(",");
                double salary = field[7].length() > 0 ? Double.parseDouble(field[7]) : 0.0;
                return new Tuple2<Employee_Key, Employee_Value>(
                        new Employee_Key(field[0], field[3], salary),
                        new Employee_Value(field[2], field[1])
                );
            }
        });

        /**
         * 使用方法repartitionandsortwithinpartition，它传递我们的自定义分区器，根据我们为key实现的Comparable接口，从而实现对数据进行排序。
         */
        JavaPairRDD<Employee_Key, Employee_Value> output = pair.repartitionAndSortWithinPartitions(new CustomEmployeePartitioner(30));
//		JavaPairRDD<Employee_Key, Employee_Value> output = pair.partitionBy(new CustomEmployeePartitioner(50)).sortByKey(new MyComparator());
        for (Tuple2<Employee_Key, Employee_Value> data : output.collect()) {
            System.out.println(data._1.getDepartment() + " " + data._1.getSalary() + " " + data._1.getName() + data._2.getJobTitle() + " " + data._2.getLastName());
        }
    }

}
