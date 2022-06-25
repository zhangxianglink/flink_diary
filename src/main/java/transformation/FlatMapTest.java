package transformation;

import dsapi.pojo.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author xiangzhang
 * @since 2022-06-25 21:21
 */
public class FlatMapTest {
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

        ds.flatMap(((Event value, Collector<List<String>> collector) -> {
            List<String> split = Arrays.asList(value.user.split(""));
            collector.collect(split);
        })).returns(new TypeHint<List<String>>() {}).print();

        env.execute();

    }

}
