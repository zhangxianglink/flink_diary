package sink;

import dsapi.pojo.Event;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @author nuc
 */
public class KafkaSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","124.222.222.46:9092");
        properties.setProperty("group.id","win_test");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("flink_test1", new SimpleStringSchema(), properties));

        SingleOutputStreamOperator<String> reuslt = stream.map(e -> e.replace(",", "-"));

        FlinkKafkaProducer<String> flink_test2 = new FlinkKafkaProducer<>("124.222.222.46:9092", "flink_test2", new SimpleStringSchema());
        reuslt.addSink(flink_test2);

        env.execute();

    }
}
