package demo;

import com.alibaba.fastjson.JSON;
import dsapi.pojo.Event;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * @author xiangzhang
 * @since 2022-09-01 14:27
 */
public class EventSchema implements SerializationSchema<Event>, DeserializationSchema<Event> {
    @Override
    public Event deserialize(byte[] message) throws IOException {
        final String str = new String(message);
        if (StringUtils.isNotEmpty(str)){
            return JSON.parseObject(str,Event.class);
        }
        return null;
    }

    @Override
    public boolean isEndOfStream(Event nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(Event element) {
        return JSON.toJSONString(element).getBytes();
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }
}
