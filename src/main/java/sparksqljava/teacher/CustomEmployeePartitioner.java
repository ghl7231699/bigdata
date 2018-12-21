package sparksqljava.teacher;

import org.apache.spark.Partitioner;

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
