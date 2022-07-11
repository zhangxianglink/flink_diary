package transformation;

import dsapi.pojo.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author xiangzhang
 * @since 2022-06-25 21:21
 */
public class MapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final DataStreamSource<Event> ds = env.fromElements(
                new Event("ppp", "./data", System.currentTimeMillis()),
                new Event("aaa", "./home", System.currentTimeMillis()),
                new Event("bbb", "./data", System.currentTimeMillis()),
                new Event("ccc", "./data", System.currentTimeMillis()),
                new Event("ddd", "./data", System.currentTimeMillis())
        );

//        ds.map(new myMapFunction()).print();
        // 无泛型，无需考虑泛型擦除
        ds.map(e -> e.getUrl());
        ds.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.getUrl().equals("./home");
            }
        }).print();

        env.execute();

    }

    public static class myMapFunction implements MapFunction<Event, String> {

        @Override
        public String map(Event value) throws Exception {
            return value.getUser() + value.getTimestamp();
        }
    }
}
