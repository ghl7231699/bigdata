package test.teacher;

import org.apache.spark.Partitioner;

/**
 * 我们的key是一个有3个值的自定义对象，所以简单的按key分区对我们来说是行不通的。
 * 因此，我们需要创建一个自定义分区器，它知道在自定义对象中使用哪个值来确定数据将流向哪个分区。
 * @author brave
 *
 */
public class CustomEmployeePartitioner extends Partitioner {

	private static final long serialVersionUID = 1876040429805834645L;
	private int numPartitions;

	public int getNumPartitions() {
		return numPartitions;
	}

	public void setNumPartitions(int numPartitions) {
		this.numPartitions = numPartitions;
	}
	
	public CustomEmployeePartitioner(int numPartitions) {
		super();
		this.numPartitions = numPartitions;
	}

	@Override
	public int getPartition(Object arg0) {
		Employee_Key emp=(Employee_Key)arg0;
		//相同部门的数据，在一个分区。
		return Math.abs(emp.getDepartment().hashCode()%getNumPartitions());
	}

	@Override
	public int numPartitions() {
		return getNumPartitions();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CustomEmployeePartitioner other = (CustomEmployeePartitioner) obj;
		if(other.getNumPartitions()==this.getNumPartitions()){
			return true;
		}
		return false;
	}
	
	

}
