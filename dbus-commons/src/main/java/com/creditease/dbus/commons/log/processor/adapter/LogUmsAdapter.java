package com.creditease.dbus.commons.log.processor.adapter;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class LogUmsAdapter implements Iterator<String> {

    private String value;

    private Iterator<String> it;


    public LogUmsAdapter(String value) {
        this.value = value;
        adapt();
    }

    private void adapt() {
        List<String> res = new ArrayList<>();
        JSONObject jsonObject = JSON.parseObject(value);
        JSONArray payload = jsonObject.getJSONArray("payload");
        JSONObject schema = jsonObject.getJSONObject("schema");
        JSONArray fields = schema.getJSONArray("fields");
        String namespace = (String) schema.get("namespace");

        List<String> fieldList = new ArrayList<>();
        for(int i = 0; i < fields.size(); i++) {
            JSONObject field = fields.getJSONObject(i);
            fieldList.add((String) field.get("name"));
        }

        for(int i = 0; i < payload.size(); i++) {
            JSONObject ret = new JSONObject();
            JSONObject tuple = payload.getJSONObject(i);
            JSONArray val = tuple.getJSONArray("tuple");
            for(int j = 0; j < val.size(); j++) {
                ret.put(fieldList.get(j), val.get(j));
            }
            ret.put("namespace", namespace);
            res.add(ret.toJSONString());
        }

        it = res.iterator();
    }

    public boolean hasNext() {
        return it.hasNext();
    }

    public String next() {
        return it.next();
    }

}
