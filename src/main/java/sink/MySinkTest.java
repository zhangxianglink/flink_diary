package sink;

import cn.hutool.db.nosql.redis.RedisDS;
import dsapi.pojo.Event;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

/**
 * @author nuc
 */
public class MySinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final DataStreamSource<Event> ds = env.fromElements(
                new Event("1", "./data", System.currentTimeMillis()),
                new Event("2", "./home", System.currentTimeMillis()),
                new Event("3", "./data", System.currentTimeMillis()),
                new Event("4", "./data", System.currentTimeMillis()),
                new Event("5", "./data", System.currentTimeMillis())
        );



        ds.addSink(new RichSinkFunction<Event>() {

            private  Jedis jedis;

            @Override
            public void open(Configuration parameters) throws Exception {
                jedis = RedisDS.create().getJedis();
            }

            @Override
            public void invoke(Event value, Context context) throws Exception {

                jedis.set(value.user,value.toString());
            }
        });

        ds.print();
        env.execute();
    }
}
