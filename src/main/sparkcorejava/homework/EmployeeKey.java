package homework;

import java.io.Serializable;

/**
 * 自定义key类 实现Comparable接口用于比较key值 从而进行排序
 */
public class EmployeeKey implements Comparable<EmployeeKey>, Serializable {

    private static final long serialVersionUID = -3049165127940796833L;
    private String name;
    private String department;
    private double salary;

    public EmployeeKey(String name, String department, double salary) {
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
    public int compareTo(EmployeeKey o) {
        if (o == null) {
            return 0;
        }
        double salary = o.getSalary();
        String name = o.getName();
        //根据salary排序
        int compare = (int) (salary - this.getSalary());
        if (compare == 0) {//salary相等则比较name
            compare = name.compareTo(this.getName());
        }
        return compare;
    }
}
