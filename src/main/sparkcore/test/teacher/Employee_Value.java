package test.teacher;

import java.io.Serializable;

/**
 * 为数据创建value类。
 *
 * @author brave
 */
public class Employee_Value implements Serializable {
    private String jobTitle;
    private String lastName;

    public Employee_Value(String jobTitle, String lastName) {
        super();
        this.jobTitle = jobTitle;
        this.lastName = lastName;
    }

    public String getJobTitle() {
        return jobTitle;
    }

    public void setJobTitle(String jobTitle) {
        this.jobTitle = jobTitle;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

}
