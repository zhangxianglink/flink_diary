package partition;

import dsapi.pojo.Event;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @author nuc
 */
public class PartitionTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> ds = environment.fromElements(
                new Event("jack", "aaa", System.currentTimeMillis()),
                new Event("made", "the bus station is over there", System.currentTimeMillis()),
                new Event("a", "can i help you ?", System.currentTimeMillis()),
                new Event("b", "can i help you ?", System.currentTimeMillis()),
                new Event("c", "can i help you ?", System.currentTimeMillis()),
                new Event("d", "can i help you ?", System.currentTimeMillis()),
        new Event("e", "can i help you ?", System.currentTimeMillis()),
                new Event("f", "can i help you ?", System.currentTimeMillis())
        );

        // 随即分区
       ds.shuffle().print().setParallelism(4);
        // 轮询分区
       ds.rebalance().print().setParallelism(4);

        // 重新分组
      environment.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                for (int i = 1; i < 9; i++) {
                    if (i %2 == getRuntimeContext().getIndexOfThisSubtask())
                        sourceContext.collect(i);
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2)
        .rescale().print().setParallelism(4);

        // 广播
        ds.broadcast().print().setParallelism(2);

        // 全局分区
        ds.global().print().setParallelism(4);

        // 自定义分区
        environment.fromElements(1,2,3,4,5,6,7,8).partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer integer, int i) {
                System.out.println(integer + "   " + i);
                return integer-1;
            }
        }, new KeySelector<Integer, Integer>() {
            @Override
            public Integer getKey(Integer integer) throws Exception {
                return integer;
            }
        }).print().setParallelism(8);
        environment.execute();
    }
}
