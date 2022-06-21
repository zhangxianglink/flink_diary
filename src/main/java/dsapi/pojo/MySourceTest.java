package dsapi.pojo;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义测试数据
 * @author xiangzhang
 * @since 2022-06-21 14:26
 */
public class MySourceTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//         The parallelism of non parallel operator must be 1.
//        DataStreamSource source = env.addSource(new MySourceFunction()).setParallelism(2);
//        source.print();

        DataStreamSource<Event> eventDataStreamSource = env.addSource(new MySourceFunction2()).setParallelism(2);
        eventDataStreamSource.print();

        env.execute();
    }
}
