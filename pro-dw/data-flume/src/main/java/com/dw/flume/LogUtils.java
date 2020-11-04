package com.dw.flume;

import org.apache.commons.lang.StringUtils;

/**
 * Ciel
 * pro-frames: com.dw.flume
 * 2020-11-04 20:07:25
 */
public class LogUtils {
    public static boolean validateStart(String msg) {
        if(StringUtils.isBlank(msg.trim())){
            return false;
        }
        if(!msg.trim().startsWith("{") || !msg.trim().endsWith("}")){
            return false;
        }
        return true;
    }

    public static boolean validateEvent(String msg) {
        if (StringUtils.isBlank(msg.trim())){

        }
        return false;
    }
}
