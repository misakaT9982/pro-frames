package com.dw.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * Ciel
 * pro-frames: com.dw.flume
 * 2020-11-04 20:11:27
 */
public class TypeInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        String body = new String(event.getBody(), Charset.forName("utf-8"));
        Map<String, String> headers = event.getHeaders();
        if(body.contains("start")){
            headers.put("topic","topic_start");
        }else{
            headers.put("topic","topic_event");
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event:events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {

    }
    public static class Buidler implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new TypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
