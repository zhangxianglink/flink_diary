package demo;

import akka.remote.serialization.StringSerializer;
import com.alibaba.fastjson.JSON;
import dsapi.pojo.Event;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author nuc
 */
public class KafkaEventProduct {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "124.222.222.46:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 30000; i < 40000; i++) {
            Event event = new Event("user" + (i % 2), "url" + i, System.currentTimeMillis());
            final RecordMetadata test = producer.send(new ProducerRecord<>("test", JSON.toJSONString(event))).get();
            System.out.println(test.offset());
            Thread.sleep(1000);
        }
        producer.close();
    }
}
