package demo;

import java.io.Serializable;
import java.time.LocalDateTime;


public class KafkaLogDTO implements Serializable {

    /**
     * class
     **/
    public String className;

    @Override
    public String toString() {
        return "KafkaLogDTO2{" +
                "className='" + className + '\'' +
                ", method='" + method + '\'' +
                ", user='" + user + '\'' +
                ", time=" + time +
                ", uri='" + uri + '\'' +
                ", browser='" + browser + '\'' +
                ", os='" + os + '\'' +
                ", ip='" + ip + '\'' +
                ", params='" + params + '\'' +
                ", module='" + module + '\'' +
                '}';
    }

    /**
     * 请求方法
     */
    public String method;

    /**
     * 操作用户(关联用户表)
     */
    public String user;


    public LocalDateTime time;


    /**
     * URI
     */
    public String uri;

    /**
     * 浏览器
     */
    public String browser;

    /**
     * 操作系统
     */
    public String os;

    /**
     * ip
     */
    public String ip;

    /**
     * 入参
     */
    public String params;

    /**
     * 模块
     */
    public String module;

    public KafkaLogDTO() {
    }

    public KafkaLogDTO(String className, String method, String user, LocalDateTime time, String uri, String browser, String os, String ip, String params, String module) {
        this.className = className;
        this.method = method;
        this.user = user;
        this.time = time;
        this.uri = uri;
        this.browser = browser;
        this.os = os;
        this.ip = ip;
        this.params = params;
        this.module = module;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public LocalDateTime getTime() {
        return time;
    }

    public void setTime(LocalDateTime time) {
        this.time = time;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getBrowser() {
        return browser;
    }

    public void setBrowser(String browser) {
        this.browser = browser;
    }

    public String getOs() {
        return os;
    }

    public void setOs(String os) {
        this.os = os;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getParams() {
        return params;
    }

    public void setParams(String params) {
        this.params = params;
    }

    public String getModule() {
        return module;
    }

    public void setModule(String module) {
        this.module = module;
    }
}
