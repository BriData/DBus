package com.creditease.dbus.commons.log.processor.adapter;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import java.util.*;



public class LogFilebeatAdapter implements Iterator<String> {
    private String value;
    private Iterator<String> it;

    public LogFilebeatAdapter(String value) {
        this.value = value;
        adapt();
    }

    private void adapt() {
        List<String> wk = new ArrayList<>();
        JSONObject ret = new JSONObject();
        JSONObject fbData = JSON.parseObject(value);
        for(String key : fbData.keySet()) {
            if(fbData.get(key) instanceof JSONObject) {
                JSONObject subJbValue = ((JSONObject) fbData.get(key));
                for(String jbKey : subJbValue.keySet()) {
                    ret.put(StringUtils.joinWith(".", key, jbKey), subJbValue.get(jbKey));
                }
            } else {
                ret.put(key, fbData.get(key));
            }
        }
        wk.add(ret.toJSONString());
        it = wk.iterator();
    }

    public boolean hasNext() {
        return it.hasNext();
    }

    public String next() {
        return it.next();
    }

}
