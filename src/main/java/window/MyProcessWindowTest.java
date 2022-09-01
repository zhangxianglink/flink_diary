package window;

import demo.EventSchema;
import dsapi.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Properties;

/**
 * @author xiangzhang
 * @since 2022-09-01 14:06
 */
public class MyProcessWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","124.222.222.46:9092");
        properties.setProperty("group.id","zx_test");
        FlinkKafkaConsumer<Event> kafkaConsumer = new FlinkKafkaConsumer<>("test",new EventSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        kafkaConsumer.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
                return element.getTimestamp();
            }
        }));


        DataStream<Event> source = env.addSource(kafkaConsumer);

        source
                .keyBy(e -> e.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .process(new MyProcessWindowFunction())
                .print();

        env.execute();

    }

    public static class MyProcessWindowFunction extends ProcessWindowFunction<Event, String,String, TimeWindow> {

        @Override
        public void process(String aBoolean, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            Long sum = 0L;
            int count = 0;
            for (Event e : elements){
                sum += e.getTimestamp();
                count ++;
            }
            out.collect("窗口："+aBoolean+" : "+sum);
        }
    }
}
