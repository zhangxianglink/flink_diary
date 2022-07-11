package transformation;

import dsapi.pojo.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author nuc
 */
public class RichMapFunctionTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> ds = environment.fromElements(
                new Event("jack", "aaa", System.currentTimeMillis()),
                new Event("made", "the bus station is over there", System.currentTimeMillis()),
                new Event("nice", "can i help you ?", System.currentTimeMillis())
        );
        ds.map(new MyRichMapFunction()).print();
        environment.execute();
    }


    public static class MyRichMapFunction extends RichMapFunction<Event,Integer>{

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open this task: " + getRuntimeContext().getIndexOfThisSubtask() + " start ---------");
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close this task: "+ getRuntimeContext().getIndexOfThisSubtask() + " end ---------");
        }

        @Override
        public Integer map(Event event) throws Exception {
            return event.url.length();
        }
    }

}

