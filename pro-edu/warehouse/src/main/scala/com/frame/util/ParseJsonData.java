package com.frame.util;

import com.alibaba.fastjson.JSONObject;

/**
 * mito
 * pro-frames: com.frame.util
 * 2020-12-08 21:15:43
 */
public class ParseJsonData {
    public  static JSONObject getJsonObject(String data){
        try {
            return JSONObject.parseObject(data);
        }catch (Exception e){
            return null;
        }
    }
}
