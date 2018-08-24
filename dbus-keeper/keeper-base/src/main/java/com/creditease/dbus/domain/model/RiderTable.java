package com.creditease.dbus.domain.model;

import com.alibaba.fastjson.annotation.JSONField;

import java.util.Date;

public class RiderTable {
    private String namespace;
    private String kafka;
    private String topic;
    private Integer id;
    @JSONField(format = "yyyy-MM-dd HH:mm:ss SSS")
    private Date createTime;

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getKafka() {
        return kafka;
    }

    public void setKafka(String kafka) {
        this.kafka = kafka;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
