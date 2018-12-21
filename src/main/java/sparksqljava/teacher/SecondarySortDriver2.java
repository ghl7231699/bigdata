package sparksqljava.teacher;

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
 * First Name
 * Last Name
 * Job Titles
 * Department
 * Full or Part-Time
 * Salary
 * Typical Hours
 * Annual Salary
 * Hourly Rate
 * 根据Department字段对数据进行分组，然后我们将首先根据salary排序，然后根据firstName排序。
 * @author Brave
 *
 */
public class SecondarySortDriver2 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("pattern").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> rdd = jsc.textFile("in/employee.csv");
		
		JavaPairRDD<Employee_Key, Employee_Value> pair = rdd.mapToPair(new PairFunction<String, Employee_Key, Employee_Value>() {
				@Override
				public Tuple2<Employee_Key, Employee_Value> call(String data) throws Exception {
					String[] field = data.split(",");
					double salary = field[7].length() > 0 ? Double.parseDouble(field[7]) : 0.0;
					return new Tuple2<Employee_Key, Employee_Value>(
					new Employee_Key(field[0], field[3],salary),
					new Employee_Value(field[2], field[1]));
				}
		});
		
		
		
		JavaPairRDD<Employee_Key, Employee_Value> output = pair.repartitionAndSortWithinPartitions(new CustomEmployeePartitioner(50));
		for (Tuple2<Employee_Key, Employee_Value> data : output.collect()) {
			System.out.println(data._1.getDepartment() + " " + data._1.getSalary() + " " + data._1.getName()+ data._2.getJobTitle() + " " + data._2.getLastName());
		}
	}

}
