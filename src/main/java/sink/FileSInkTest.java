package sink;

import dsapi.pojo.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author nuc
 */
public class FileSInkTest {
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

        StreamingFileSink<String> sink = StreamingFileSink.forRowFormat(new Path("./outputPath"), new SimpleStringEncoder<String>("UTF-8"))
                // 滚动策略
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();

        ds.map(e -> e.toString()).addSink(sink).setParallelism(4);

        env.execute();
    }
}
