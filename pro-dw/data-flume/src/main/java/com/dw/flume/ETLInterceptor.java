package com.dw.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;

/**
 * Ciel
 * pro-frames: com.dw.flume
 * 2020-11-04 20:03:50
 */
public class ETLInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        if (event == null) {
            return null;
        }
        byte[] body = event.getBody();
        String msg = new String(body, Charset.forName("utf-8"));
        if (msg.contains("\"en\":\"start\"")) { // 启动日志
            if (LogUtils.validateStart(msg)) {
                return event;
            }
        } else {
            if (LogUtils.validateEvent(msg)) {
                return event;
            }
        }
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        Iterator<Event> iterator = events.iterator();
        while (iterator.hasNext()) {
            Event next = iterator.next();
            if (intercept(events) == null) { // 舍弃空字符串
                iterator.remove();
            }
        }
        return events;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
