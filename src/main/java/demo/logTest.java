package demo;

import com.alibaba.fastjson.JSONObject;
import dsapi.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Properties;

/**
 * @author xiangzhang
 * @since 2022-08-16 15:50
 */
public class logTest {
    public static void main(String[] args) throws Exception {
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","xx:9092");
        properties.setProperty("group.id","zx_test");
        properties.put("auto.offset.reset","latest");
        FlinkKafkaConsumer<ObjectNode> kafkaConsumer = new FlinkKafkaConsumer<>("hootin_access_log", new JSONKeyValueDeserializationSchema(true), properties);
        kafkaConsumer.assignTimestampsAndWatermarks(
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2L))
        );

        DataStreamSource<ObjectNode> source = env.addSource(kafkaConsumer);
        source.map(new MapFunction<ObjectNode, KafkaLogDTO>() {

            @Override
            public KafkaLogDTO map(ObjectNode value) throws Exception {
                JsonNode node = value.get("value");
                KafkaLogDTO dto = new KafkaLogDTO();
                dto.setUri(node.get("uri").toString());
                dto.setIp(node.get("ip").toString());
                return dto;
            }
        }).keyBy(e -> e.getIp())
                .window(TumblingEventTimeWindows.of(Time.seconds(20)))
                // 入参 累加器 出参
                .aggregate(new AggregateFunction<KafkaLogDTO, Tuple3<String,String, Integer>, Tuple3<String,String, Integer>>() {

                    // 初始化累加器
                    @Override
                    public Tuple3<String, String, Integer> createAccumulator() {
                        return Tuple3.of("","",0);
                    }

                    // 累加逻辑
                    @Override
                    public Tuple3<String, String, Integer> add(KafkaLogDTO value, Tuple3<String,String, Integer>accumulator) {
                        return Tuple3.of(value.ip, accumulator.f1 + value.uri, accumulator.f2 + 1);
                    }

                    // 计算结果
                    @Override
                    public  Tuple3<String,String, Integer> getResult(Tuple3<String, String, Integer> accumulator) {
                        return Tuple3.of(accumulator.f0, accumulator.f1  , accumulator.f2);
                    }

                    // 回话窗口使用
                    @Override
                    public Tuple3<String,String, Integer> merge(Tuple3<String,String, Integer> a, Tuple3<String,String, Integer> b) {
                        return Tuple3.of("","",0);
                    }
                }).print();

        env.execute();
    }
}
