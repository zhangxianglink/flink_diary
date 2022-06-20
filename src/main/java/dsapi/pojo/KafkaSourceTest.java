package dsapi.pojo;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * socket无界流测试
 * @author xiangzhang
 * @since 2022-06-18 20:23
 */
public class KafkaSourceTest {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","124.222.222.46:9092");
        properties.setProperty("group.id","win_test");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("auto.offset.reset","latest");
        final FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("flink_test1", new SimpleStringSchema(), properties);
        final DataStreamSource<String> stream
                = env.addSource(kafkaConsumer);

        stream.print("kafka");
        env.execute();
    }
}
