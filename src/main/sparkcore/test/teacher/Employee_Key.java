package test.teacher;

import java.io.Serializable;

/**
 * 为数据创建key类。Employee_Key类应该实现可比的接口，这样Spark框架就知道如何对自定义对象进行排序。
 *
 * @author Brave
 */
public class Employee_Key implements Comparable<Employee_Key>, Serializable {
    private static final long serialVersionUID = -3049165127940796833L;
    private String name;
    private String department;
    private double salary;

    public Employee_Key(String name, String department, double salary) {
        super();
        this.name = name;
        this.department = department;
        this.salary = salary;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDepartment() {
        return department;
    }

    public void setDepartment(String department) {
        this.department = department;
    }

    public double getSalary() {
        return salary;
    }

    public void setSalary(double salary) {
        this.salary = salary;
    }

    @Override
    public int compareTo(Employee_Key emp1) {
//		System.out.println("emp1.getSalary:"+emp1.getSalary()+",this.getSalary()"+this.getSalary());
        //先按salary排序
        int compare = (int) emp1.getSalary() - (int) this.getSalary();
        //如果salary相同,再按name排序
        if (compare == 0) {
            compare = this.getName().compareTo(emp1.getName());
        }
        return compare;
    }

}
