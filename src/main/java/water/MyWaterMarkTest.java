package water;

import dsapi.pojo.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @author nuc
 */
public class MyWaterMarkTest {

    // 水位线意义 按照事件时间进行任务的处理
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> stream = env.fromElements(new Event("jack", "rol", System.currentTimeMillis()), new Event("jack", "rol", System.currentTimeMillis()));
        // 有序流的生成策略
        stream.assignTimestampsAndWatermarks(WatermarkStrategy
                //  顺序生成水位线
                .<Event>forMonotonousTimestamps()
                // 生成时间线
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        // 默认毫秒数
                        return element.timestamp;
                    }
                }));
       stream.assignTimestampsAndWatermarks(
                // 无序流 + 延迟时长 , 其他一致
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1L))
               .withTimestampAssigner((e,t) -> e.timestamp));


        env.execute();
    }
}
