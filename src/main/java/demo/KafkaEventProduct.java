package demo;

import akka.remote.serialization.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

/**
 * @author nuc
 */
public class KafkaEventProduct {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "master:9092,slave1:9092,slave2:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 100000; i++) {
            producer.send()
        }
    }
}
