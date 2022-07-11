package transformation;

import dsapi.pojo.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author xiangzhang
 * @since 2022-06-27 22:00
 */
public class  MaxTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final DataStreamSource<Event> ds = env.fromElements(
                new Event("ppp", "./data", System.currentTimeMillis() + 9000L),
                new Event("aaa", "./home", System.currentTimeMillis()+ 1000L),
                new Event("bbb", "./data", System.currentTimeMillis() + 1000L),
                new Event("ccc", "./data", System.currentTimeMillis()+ 10000L),
                new Event("ddd", "./data", System.currentTimeMillis()+ 2000L)
        );

        final KeyedStream<Event, String> keyedStream = ds.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event value) throws Exception {
                return value.url;
            }
        });
        keyedStream.max("timestamp").print();

        env.execute();
    }
}
