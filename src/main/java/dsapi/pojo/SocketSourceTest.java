package dsapi.pojo;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * socket无界流测试
 * @author xiangzhang
 * @since 2022-06-18 20:23
 */
public class SocketSourceTest {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final DataStreamSource<String> stream = env.socketTextStream("124.222.222.46", 8080);
        stream.print();

        env.execute();
    }
}
