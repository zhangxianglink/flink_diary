package window.agg;

import dsapi.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author nuc
 */
public class WindowAggregateTest {

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
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        streamOperator
                .keyBy(e -> e.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 入参 累加器 出参
                .aggregate(new AggregateFunction<Event, Tuple3<String,Long, Integer>, Tuple2<String, Long>>() {

                    // 初始化累加器
                    @Override
                    public Tuple3<String, Long, Integer> createAccumulator() {
                        return Tuple3.of("",0L,0);
                    }

                    // 累加逻辑
                    @Override
                    public Tuple3<String, Long, Integer> add(Event value, Tuple3<String,Long, Integer>accumulator) {
                        return Tuple3.of(value.user, accumulator.f1 + value.timestamp, accumulator.f2 + 1);
                    }

                    // 计算结果
                    @Override
                    public Tuple2<String, Long> getResult(Tuple3<String, Long, Integer> accumulator) {
                        return Tuple2.of(accumulator.f0, accumulator.f1  / accumulator.f2);
                    }

                    // 回话窗口使用
                    @Override
                    public Tuple3<String,Long, Integer> merge(Tuple3<String,Long, Integer> a, Tuple3<String,Long, Integer> b) {
                        return Tuple3.of(a.f0, a.f1 + b.f1, b.f2 + a.f2);
                    }
                }).print();

        env.execute();

    }

    /**
     (n1,4666)
     (n3,6000)
     (n2,4500)
     */
}
