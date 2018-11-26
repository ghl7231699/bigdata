package homework;


import org.apache.spark.Partitioner;

public class CustomerPartitions extends Partitioner {
    private static final long serialVersionUID = 1876040429805834645L;
    private int numPartitions;

    public CustomerPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    @Override
    public int numPartitions() {
        return numPartitions;
    }

    @Override
    public int getPartition(Object key) {
        EmployeeKey key1 = (EmployeeKey) key;
        String department = key1.getDepartment();
        //相同部门的数据，放在在一个分区。
        return Math.abs(department.hashCode() % numPartitions);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (this == obj) {
            return true;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        CustomerPartitions cp = (CustomerPartitions) obj;

        if (cp.getNumPartitions() == numPartitions) {//分区相同
            return true;
        }
        return false;
    }
}
