package com.creditease.dbus.heartbeat.distributed;

import com.alibaba.fastjson.JSON;

public class Worker {

    private int id;
    private String host;

    public Worker(int id, String host) {
        this.id = id;
        this.host = host;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
