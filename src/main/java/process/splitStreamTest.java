package process;

import dsapi.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


import java.time.Duration;

public class splitStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> stream = env.fromElements(new Event("jack", "rol1", System.currentTimeMillis()),
                                                         new Event("mark", "url2", System.currentTimeMillis() + 1000L),
                                                         new Event("jack", "url2", System.currentTimeMillis() + 2000L),
                                                         new Event("kk", "url2", System.currentTimeMillis() + 3000L),
                                                         new Event("jack", "url2", System.currentTimeMillis() + 4000L),
                                                         new Event("kk", "url2", System.currentTimeMillis() + 5000L));
        stream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestamp;
                    }
                }));

        OutputTag<Tuple3<String, String, Long>> tuple1 =new OutputTag<Tuple3<String, String, Long>>("jack"){};
        OutputTag<Tuple3<String, String, Long>> tuple2 =new OutputTag<Tuple3<String, String, Long>>("kk"){};
        SingleOutputStreamOperator<Event> process = stream.process(new ProcessFunction<Event, Event>() {

            @Override
            public void processElement(Event event, ProcessFunction<Event, Event>.Context context, Collector<Event> collector) throws Exception {
                if (event.getUser().equals("jack")) {
                    context.output(tuple1,  Tuple3.of(event.getUrl(), event.getUser(), event.getTimestamp()));
                } else if (event.getUser().equals("kk")) {
                    context.output(tuple2, Tuple3.of(event.getUrl(), event.getUser(), event.getTimestamp()));
                } else {
                    collector.collect(event);
                }
            }
        });

        process.print("else");
        process.getSideOutput(tuple1).print("jack");
        process.getSideOutput(tuple2).print("kk");
        env.execute();
    }
}
