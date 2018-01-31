package com.creditease.dbus.log.processor.window;


public class StatWindowInfo extends Element {

    private Long successCnt = 0L;

    private Long errorCnt = 0L;

    private Long readKafkaCount = 0L;

    public Long getSuccessCnt() {
        return successCnt;
    }

    public void setSuccessCnt(Long successCnt) {
        this.successCnt = successCnt;
    }

    public Long getErrorCnt() {
        return errorCnt;
    }

    public void setErrorCnt(Long errorCnt) {
        this.errorCnt = errorCnt;
    }

    public Long getReadKafkaCount() {
        return readKafkaCount;
    }

    public void setReadKafkaCount(Long readKafkaCount) {
        this.readKafkaCount = readKafkaCount;
    }


    @Override
    public void merge(Element e, Integer taskIdSum) {
        super.merge(e, taskIdSum);
        this.successCnt += ((StatWindowInfo) e).getSuccessCnt();
        this.errorCnt += ((StatWindowInfo) e).getErrorCnt();
        this.readKafkaCount += ((StatWindowInfo) e).getReadKafkaCount();
    }
}
