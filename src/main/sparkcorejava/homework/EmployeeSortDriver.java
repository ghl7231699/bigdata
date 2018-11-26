package homework;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

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
public class EmployeeSortDriver {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("pattern").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile("in/employee_all.csv");
        sc.setLogLevel("ERROR");

        JavaPairRDD<EmployeeKey, EmployeeValue> pairRdd = rdd.mapToPair(new PairFunction<String, EmployeeKey, EmployeeValue>() {
            @Override
            public Tuple2<EmployeeKey, EmployeeValue> call(String s) throws Exception {
                String[] split = s.split(",");
                double salary = split[7].length() > 0 ? Double.parseDouble(split[7]) : 0.0;

                return new Tuple2<>(new EmployeeKey(split[0], split[3], salary), new EmployeeValue(s));
            }
        });
        System.out.println(pairRdd);
        System.out.println("*************************");

        pairRdd.cache();
        //todo 优化：分区数的设置 会导致最终结果的差异性：分区数的设置应该根据文件动态设置
        List<Tuple2<EmployeeKey, EmployeeValue>> collect = pairRdd.repartitionAndSortWithinPartitions(new CustomerPartitions(50))
                .collect();
        for (int i = 0; i < collect.size(); i++) {
            Tuple2<EmployeeKey, EmployeeValue> tuple2 = collect.get(i);
            System.out.println("result are :\t" + tuple2._1.getDepartment() + "\t" + tuple2._1.getSalary() + "\t" + tuple2._1.getName() + ":\t" + tuple2._2().getContent());
        }
    }
}
