package com.creditease.dbus.log.processor.vo;

import java.util.List;
import java.util.Map;


public class RecordWrapper {
    private Long timeStamp;
    private Integer partition;
    private Long offset;
    private List<String> value;
    private Map<String, String> recordMap;


    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

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

    public List<String> getValue() {
        return value;
    }

    public void setValue(List<String> value) {
        this.value = value;
    }

    public Map<String, String> getRecordMap() {
        return recordMap;
    }

    public void setRecordMap(Map<String, String> recordMap) {
        this.recordMap = recordMap;
    }
}
