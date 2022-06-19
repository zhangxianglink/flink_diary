package dsapi.pojo;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

/**
 * 本地有界流测试
 * @author xiangzhang
 * @since 2022-06-18 20:23
 */
public class SourceTest {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 文件读取
        final DataStreamSource<String> stream1 = env.readTextFile("input/clicks.txt");
        // 集合读取
        final ArrayList<Integer> list = new ArrayList<>();
        list.add(1);
        final DataStreamSource<Integer> stream2 = env.fromCollection(list);

        final ArrayList<Event> list2 = new ArrayList<>();
        list2.add(new Event("mary","./home",1000L));
        final DataStreamSource<Event> stream3 = env.fromCollection(list2);

        // 从元素读取
        final DataStreamSource<Event> stream4 = env.fromElements(
                new Event("mary", "./home", 1000L)
        );

        stream1.print();
        stream2.print("stream2");
        stream3.print("stream3");
        stream4.print("stream4");

        env.execute();
    }
}
