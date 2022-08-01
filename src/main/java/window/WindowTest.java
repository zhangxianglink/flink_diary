package window;

import dsapi.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author nuc
 */
public class WindowTest {

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


        stream.keyBy(e -> e.user)
                // 计数滑动
                .countWindow(10,2);
                // 计数滚动
//                .countWindow(10);
                // 事件时间回话窗口
//                .window(EventTimeSessionWindows.withGap(Time.seconds(10)));
                // 滑动事件时间窗口 size: 窗口大学 , slide: 滑动距离， offset：偏移量
//                .window(SlidingEventTimeWindows.of(Time.seconds(5L),Time.seconds(1L)));
                // 滚动事件时间窗口
//                .window(TumblingEventTimeWindows.of(Time.seconds(1L)));


        env.execute();
    }

}
