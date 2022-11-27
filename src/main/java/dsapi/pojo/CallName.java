package dsapi.pojo;

import java.io.Serializable;

/**
 * @author nuc
 */
public class CallName implements Serializable {

    public String name;


    public CallName(String name) {
        this.name = name;
    }

    public CallName() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "CallName{" +
                "name='" + name + '\'' +
                '}';
    }
}
