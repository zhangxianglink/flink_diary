package sink;



import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import sink.pojo.ToMysqlBean1;
import sink.pojo.ToMysqlBean2;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Optional;

/**
 * caoyu
 * Create in 2023/3/3
 */
public class FileToMysqlTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("input/user.txt");
        env.setParallelism(1);

        SingleOutputStreamOperator<Object> streamOperator = stringDataStreamSource.flatMap(new FlatMapFunction<String, Object>() {
            @Override
            public void flatMap(String row, Collector<Object> collector) throws Exception {
                String[] split = row.split("\\|");
                if (split.length < 4) {
                    collector.collect(new ToMysqlBean1(
                            split[0],
                            split[1],
                            split[2]));
                } else {
                    collector.collect(new ToMysqlBean2(
                            split[0],
                            split[1],
                            split[2],
                            split[3],
                            split[4],
                            split[5]
                    ));
                }
            }
        });
        SinkFunction<Object> root = JdbcSink.sink("insert into tooltt (id, title, author, price, qty, xx) values (?,?,?,?,?,?)",
                (ps, t) -> {
                    if (t instanceof ToMysqlBean1) {
                        ps.setString(1, ((ToMysqlBean1) t).getA());
                        ps.setString(2, ((ToMysqlBean1) t).getB());
                        ps.setString(3, ((ToMysqlBean1) t).getC());
                        ps.setString(4, "");
                        ps.setString(5, "");
                        ps.setString(6, "");
                    } else {
                        ps.setString(1, ((ToMysqlBean2) t).getA1());
                        ps.setString(2, ((ToMysqlBean2) t).getA2());
                        ps.setString(3, ((ToMysqlBean2) t).getA3());
                        ps.setString(4, ((ToMysqlBean2) t).getA4());
                        ps.setString(5, ((ToMysqlBean2) t).getA5());
                        ps.setString(6, ((ToMysqlBean2) t).getA6());
                    }
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://:3306/test")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456")
                        .build());
//        streamOperator.addSink(root);
//        env.execute("fileToMysql: ");

        streamOperator.addSink(new RichSinkFunction<Object>() {
            Connection con;
            PreparedStatement p1;
            PreparedStatement p2;

            @Override
            public void open(Configuration parameters) throws Exception {
                Class.forName("com.mysql.cj.jdbc.Driver");
                //3、获取数据库的连接对象
                con = DriverManager.getConnection("jdbc:mysql://124.222.222.46:3306/test", "root", "123456");
                p1 = con.prepareStatement("insert into book1 (id, title, author, price, qty, xx) values (?,?,?,?,?,?)");
                p2 = con.prepareStatement("insert into book2 (id, title, author) values (?,?,?)");

            }

            @Override
            public void invoke(Object t, Context context) throws Exception {
                if (t instanceof ToMysqlBean1) {
                    p2.setString(1, ((ToMysqlBean1) t).getA());
                    p2.setString(2, ((ToMysqlBean1) t).getB());
                    p2.setString(3, ((ToMysqlBean1) t).getC());
                    p2.execute();
                } else {
                    p1.setString(1, ((ToMysqlBean2) t).getA1());
                    p1.setString(2, ((ToMysqlBean2) t).getA2());
                    p1.setString(3, ((ToMysqlBean2) t).getA3());
                    p1.setString(4, ((ToMysqlBean2) t).getA4());
                    p1.setString(5, ((ToMysqlBean2) t).getA5());
                    p1.setString(6, ((ToMysqlBean2) t).getA6());
                    p1.execute();
                }
            }

            @Override
            public void close() throws Exception {
                p1.close();
                p2.close();
                con.close();
            }
        });

       env.execute("fileToMysql2: ");
    }

}
