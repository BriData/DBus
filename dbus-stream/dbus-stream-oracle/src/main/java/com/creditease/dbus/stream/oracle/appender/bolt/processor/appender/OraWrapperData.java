package com.creditease.dbus.stream.oracle.appender.bolt.processor.appender;

import com.creditease.dbus.stream.common.appender.utils.PairWrapper;

/**
 * Created by zhangyf on 18/1/5.
 */
public class OraWrapperData {
    private PairWrapper<String, Object> dataWrapper;
    private PairWrapper<String, Object> beforeDataWrapper;

    public OraWrapperData() {
    }

    public OraWrapperData(PairWrapper<String, Object> dataWrapper, PairWrapper<String, Object> beforeDataWrapper) {
        this.dataWrapper = dataWrapper;
        this.beforeDataWrapper = beforeDataWrapper;
    }

    public PairWrapper<String, Object> getDataWrapper() {
        return dataWrapper;
    }

    public void setDataWrapper(PairWrapper<String, Object> dataWrapper) {
        this.dataWrapper = dataWrapper;
    }

    public PairWrapper<String, Object> getBeforeDataWrapper() {
        return beforeDataWrapper;
    }

    public void setBeforeDataWrapper(PairWrapper<String, Object> beforeDataWrapper) {
        this.beforeDataWrapper = beforeDataWrapper;
    }
}
