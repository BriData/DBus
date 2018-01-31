package com.creditease.dbus.log.processor.vo;

import java.io.Serializable;

/**
 * Created by Administrator on 2017/9/28.
 */
public class DBusDataSource implements Serializable {

    private long id;
    private String dsName;
    private String dsType;
    private String topic;
    private String controlTopic;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getDsName() {
        return dsName;
    }

    public void setDsName(String dsName) {
        this.dsName = dsName;
    }

    public String getDsType() {
        return dsType;
    }

    public void setDsType(String dsType) {
        this.dsType = dsType;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getControlTopic() {
        return controlTopic;
    }

    public void setControlTopic(String controlTopic) {
        this.controlTopic = controlTopic;
    }

    public String toString() {
        return this.id + " " + this.dsName + " " + this.dsType + " " + this.topic + " " + this.controlTopic;
    }
}
