package homework;

import java.io.Serializable;

/**
 *
 */
public class EmployeeValue implements Serializable {
    private String content;

    public EmployeeValue(String content) {
        this.content = content;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
