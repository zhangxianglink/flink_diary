package sink.pojo;

public class ToMysqlBean1 {
    public String A;
    public String B;

    public ToMysqlBean1(String a, String b, String c) {
        A = a;
        B = b;
        C = c;
    }

    public String C;

    public ToMysqlBean1() {
    }

    @Override
    public String toString() {
        return "ToMysqlBean1{" +
                "A='" + A + '\'' +
                ", B='" + B + '\'' +
                ", C='" + C + '\'' +
                '}';
    }

    public String getA() {
        return A;
    }

    public void setA(String a) {
        A = a;
    }

    public String getB() {
        return B;
    }

    public void setB(String b) {
        B = b;
    }

    public String getC() {
        return C;
    }

    public void setC(String c) {
        C = c;
    }
}
