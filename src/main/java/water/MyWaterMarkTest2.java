package water;

import dsapi.pojo.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @author nuc
 */
public class MyWaterMarkTest2 {

    // 水位线意义 按照事件时间进行任务的处理
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> stream = env.fromElements(new Event("jack", "rol", System.currentTimeMillis()), new Event("jack", "rol", System.currentTimeMillis()));
        stream.assignTimestampsAndWatermarks(new WatermarkStrategy<Event>() {
            @Override
            public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                return (e,t) -> e.timestamp;
            }

            @Override
            public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new BoundedOutOfOrdernessGenerator();
            }
        });
        env.execute();
    }

    /**
     * 该 watermark 生成器可以覆盖的场景是：数据源在一定程度上乱序。
     * 即某个最新到达的时间戳为 t 的元素将在最早到达的时间戳为 t 的元素之后最多 n 毫秒到达。
     */
    static class BoundedOutOfOrdernessGenerator implements WatermarkGenerator<Event> {
        private final long maxOutOfOrderness = 3500; // 3.5 秒

        private long currentMaxTimestamp;


        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 发出的 watermark = 当前最大时间戳 - 最大乱序时间
            output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1));
        }
    }

    /**
     * 该生成器生成的 watermark 滞后于处理时间固定量。它假定元素会在有限延迟后到达 Flink。
     */
    public class TimeLagWatermarkGenerator implements WatermarkGenerator<Event> {

        private final long maxTimeLag = 5000; // 5 秒

        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            // 处理时间场景下不需要实现
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(System.currentTimeMillis() - maxTimeLag));
        }
    }

    /**
     * 标记 watermark 生成器观察流事件数据并在获取到带有 watermark 信息的特殊事件元素时发出 watermark。
     *
     * 如下是实现标记生成器的方法，当事件带有某个指定标记时，该生成器就会发出 watermark：
     */
    public class PunctuatedAssigner implements WatermarkGenerator<Event> {

        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            if (event.user.equals("jack")) {
                output.emitWatermark(new Watermark(event.getTimestamp()));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // onEvent 中已经实现
        }
    }
}
