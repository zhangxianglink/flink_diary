package dsapi.pojo;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author xiangzhang
 * @since 2022-06-21 14:27
 */
public class MySourceFunction2 implements ParallelSourceFunction<Event> {

    private  boolean running = true;

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        while (running){
            sourceContext.collect(new Event("a","b",System.currentTimeMillis()));
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
