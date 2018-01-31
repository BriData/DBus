package com.creditease.dbus.log.processor.window;

import com.creditease.dbus.commons.DbusMessage;
import com.creditease.dbus.log.processor.window.Element;

/**
 * Created by zhenlinzhong on 2017/11/15.
 */
public class HeartBeatWindowInfo extends Element {
    private Integer partition;
    private Long offset;
    private DbusMessage dbusMessage;
    private String status;
    private String outputTopic;

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public DbusMessage getDbusMessage() {
        return dbusMessage;
    }

    public void setDbusMessage(DbusMessage dbusMessage) {
        this.dbusMessage = dbusMessage;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getOutputTopic() {
        return outputTopic;
    }

    public void setOutputTopic(String outputTopic) {
        this.outputTopic = outputTopic;
    }
}
