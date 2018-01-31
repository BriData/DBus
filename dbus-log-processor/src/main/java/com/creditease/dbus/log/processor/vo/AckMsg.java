package com.creditease.dbus.log.processor.vo;

import java.io.Serializable;

/**
 * Created by Administrator on 2017/10/12.
 */
public class AckMsg implements Serializable {
    private Integer partition;
    private String topic;
    private Long maxOffset;
    private Long minOffset;

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(Long maxOffset) {
        this.maxOffset = maxOffset;
    }

    public Long getMinOffset() {
        return minOffset;
    }

    public void setMinOffset(Long minOffset) {
        this.minOffset = minOffset;
    }
}
