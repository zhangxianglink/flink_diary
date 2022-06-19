package dsapi.pojo;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * 字段共有，空参构造，序列化
 * @author xiangzhang
 * @since 2022-06-17 18:04
 */
public class Event implements Serializable {

    public String user;

    public Event() {
    }

    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    public String url;
    public Long timestamp;

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
