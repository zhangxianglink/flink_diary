package word;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author xiangzhang
 * 批流统一，运行指定模式
 * @since 2022-06-12 15:27
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        // 有界流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取启动参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final String host = parameterTool.get("host");
        final int port = parameterTool.getInt("port");

        final DataStreamSource<String> ds = env.socketTextStream(host, port);

        //转换
        final SingleOutputStreamOperator<Tuple2<String, Long>> wordOneTuple = ds.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            final String[] s = line.split(" ");
            for (String word :
                    s) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        final KeyedStream<Tuple2<String, Long>, String> keyedStream = wordOneTuple.keyBy(e -> e.f0);

        final SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyedStream.sum(1);

        sum.print();

        // 多线程并行运行
        // 并行子任务，取决于并行度（默认当前cpu核心数 ）
        env.execute();
    }


}
