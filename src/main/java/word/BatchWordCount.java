package word;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * @author xiangzhang
 * @since 2022-06-11 21:19
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        // 执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 获取数据源
        DataSource<String> stringDataSource = env.readTextFile("input/word.txt");
        // 分词二元组 （a:1,b:3）
        final FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = stringDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] split = line.split(" ");
            for (String word :
                    split) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        // 按照word进行分组
        final UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOne.groupBy(0);

        // 组内累加统计
        final AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);

         sum.print();

    }
}
