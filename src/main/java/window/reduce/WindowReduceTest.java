package window.reduce;

import dsapi.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author nuc
 */
public class WindowReduceTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> dataStreamSource = env.fromElements(
                new Event("n1", "n", 1000L),
                new Event("n1", "n", 2000L),
                new Event("n1", "n", 3000L),
                new Event("n2", "n", 3000L),
                new Event("n3", "n", 4000L),
                new Event("n1", "n", 5000L),
                new Event("n3", "n", 7000L),
                new Event("n2", "n", 6000L),
                new Event("n1", "n", 8000L),
                new Event("n3", "n", 7000L),
                new Event("n1", "n", 9000L));
        SingleOutputStreamOperator<Event> streamOperator = dataStreamSource
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        streamOperator.map(new MapFunction<Event, Tuple2<String,Long>>() {
            @Override
            public Tuple2<String, Long> map(Event value) throws Exception {
                return Tuple2.of(value.user, 1L);
            }
        })
                .keyBy(e -> e.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(8L)))
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                }).print();

        env.execute();

    }

    /**
     (n1,4)
     (n2,2)
     (n3,3) 延迟一秒，追回一条n3 数据

     (n1,2) n1 进入新窗口
     */
}
