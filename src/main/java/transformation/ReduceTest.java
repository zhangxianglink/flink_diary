package transformation;

import dsapi.pojo.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.collection.generic.MapFactory;

/**
 * @author xiangzhang
 * @since 2022-06-27 22:00
 */
public class ReduceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final DataStreamSource<Event> ds = env.fromElements(
                new Event("ppp", "./data", System.currentTimeMillis() + 9000L),
                new Event("aaa", "./home", System.currentTimeMillis()+ 1000L),
                new Event("ppp", "./data", System.currentTimeMillis() + 1000L),
                new Event("aaa", "./data", System.currentTimeMillis()+ 10000L),
                new Event("ppp", "./data", System.currentTimeMillis()+ 2000L)
        );

        final SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = ds.map(new MapFunction<Event, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(Event value) throws Exception {
                return Tuple2.of(value.user, 1);
            }
        }).keyBy(e -> e.f0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        });

        reduce.keyBy(data -> "key")
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return value1.f1 > value2.f1 ? value1 : value2;
                    }
                }).print();

        env.execute();
    }
}
