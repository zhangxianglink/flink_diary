package sink.pojo;

public class ToMysqlBean2 {
    public String A1;
    public String A2;
    public String A3;
    public String A4;
    public String A5;
    public String A6;

    public ToMysqlBean2(String a1, String a2, String a3, String a4, String a5, String a6) {
        A1 = a1;
        A2 = a2;
        A3 = a3;
        A4 = a4;
        A5 = a5;
        A6 = a6;
    }

    public ToMysqlBean2() {
    }

    @Override
    public String toString() {
        return "ToMysqlBean2{" +
                "A1='" + A1 + '\'' +
                ", A2='" + A2 + '\'' +
                ", A3='" + A3 + '\'' +
                ", A4='" + A4 + '\'' +
                ", A5='" + A5 + '\'' +
                ", A6='" + A6 + '\'' +
                '}';
    }

    public String getA1() {
        return A1;
    }

    public void setA1(String a1) {
        A1 = a1;
    }

    public String getA2() {
        return A2;
    }

    public void setA2(String a2) {
        A2 = a2;
    }

    public String getA3() {
        return A3;
    }

    public void setA3(String a3) {
        A3 = a3;
    }

    public String getA4() {
        return A4;
    }

    public void setA4(String a4) {
        A4 = a4;
    }

    public String getA5() {
        return A5;
    }

    public void setA5(String a5) {
        A5 = a5;
    }

    public String getA6() {
        return A6;
    }

    public void setA6(String a6) {
        A6 = a6;
    }
}
