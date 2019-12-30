/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2019 Bridata
 * ==
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * >>
 */


package com.creditease.dbus.commons;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;


/**
 * Created by dongwang47 on 2016/8/22.
 */
public class StatMessage {

    public static final String DISPATCH_TYPE = "DISPATCH_TYPE";


    private String dsName;
    private String schemaName;
    private String tableName;
    private String type;

    //心跳实际发生时间
    private long checkpointMS;

    //心跳批次时间
    private long txTimeMS;

    //收到消息本地时间
    private long localMS;

    //localMS - checkpointMS
    private long latencyMS;
    private long count;
    private long errorCount;
    private long warningCount;

    private long offset;

    @JSONField(format = "yyyy-MM-dd HH:mm:ss.SSS")
    private Date checkpointTime;

    @JSONField(format = "yyyy-MM-dd HH:mm:ss.SSS")
    private Date localTime;
    private Map<String, Object> payload;

    //private Date date = new Date();
    //private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");

    public static StatMessage parse(String jsonString) {
        return JSON.parseObject(jsonString, StatMessage.class);
    }

    public String toJSONString() {
        compute();
        return JSON.toJSONString(this);
    }

    public StatMessage() {
    }

    public StatMessage(String dsName, String schemaName, String tableName, String type) {
        this.dsName = dsName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.type = type;
    }

    public void addCount(long i) {
        this.count += i;
    }

    public void addPayload(String key, Object value) {
        if (this.payload == null) {
            this.payload = new HashMap<>();
        }
        this.payload.put(key, value);
    }

    public <T> T payloadValue(String key, Class<T> clazz) {
        if (this.payload == null) {
            return null;
        } else {
            Object val = payload.get(key);
            if (val != null && clazz != null && clazz.isInstance(val)) {
                return (T) val;
            }
            return null;
        }
    }

    private void compute() {
        this.checkpointTime = new Date(this.checkpointMS);
        this.localTime = new Date(this.localMS);

        this.latencyMS = localMS - checkpointMS;
    }

    public void cleanUp() {

        this.checkpointMS = 0;
        this.txTimeMS = 0;
        this.localMS = 0;
        this.latencyMS = 0;
        this.checkpointTime = null;
        this.localTime = null;
        this.count = 0;
        this.errorCount = 0;
        this.warningCount = 0;
    }

    public static void main(String[] args) {
        StatMessage msg = new StatMessage("ds1", "schema1", "table1", "DispatcherType");

        msg.setCheckpointMS(System.currentTimeMillis());
        msg.setLocalMS(System.currentTimeMillis());
        msg.setCount(123);
        msg.setOffset(6666666);
        msg.addPayload("key1", "value1");

        String jsonString = msg.toJSONString();

        StatMessage obj = StatMessage.parse(jsonString);

        String type = obj.getType();
    }


    public String getDsName() {
        return dsName;
    }

    public void setDsName(String dsName) {
        this.dsName = dsName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public void addErrorCount(long i) {
        this.errorCount += i;
    }

    public long getErrorCount() {
        return errorCount;
    }

    public void setErrorCount(long errorCount) {
        this.errorCount = errorCount;
    }

    public void addWarningCount(long i) {
        this.warningCount += i;
    }

    public long getWarningCount() {
        return warningCount;
    }

    public void setWarningCount(long warningCount) {
        this.warningCount = warningCount;
    }


    public Date getCheckpointTime() {
        return checkpointTime;
    }

    public void setCheckpointTime(Date checkpointTime) {
        this.checkpointTime = checkpointTime;
    }

    public long getCheckpointMS() {
        return checkpointMS;
    }

    public void setEarlyCheckpointMS(long checkpointMS) {
        if (this.checkpointMS == 0 || this.checkpointMS > checkpointMS)
            this.checkpointMS = checkpointMS;
    }

    public void setCheckpointMS(long checkpointMS) {
        this.checkpointMS = checkpointMS;
    }

    public long getLatencyMS() {
        return latencyMS;
    }

    public long getTxTimeMS() {
        return txTimeMS;
    }

    public void setEarlyTxTimeMS(long txTimeMS) {
        if (this.txTimeMS == 0 || this.txTimeMS > txTimeMS)
            this.txTimeMS = txTimeMS;
    }

    public void setTxTimeMS(long txTimeMS) {
        this.txTimeMS = txTimeMS;
    }

    public void setLatencyMS(long latencyMS) {
        this.latencyMS = latencyMS;
    }

    public void setLocalTime(Date localTime) {
        this.localTime = localTime;
    }

    public Date getLocalTime() {
        return localTime;
    }

    public long getLocalMS() {
        return localMS;
    }

    public void setLocalMS(long localMS) {
        this.localMS = localMS;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }

    public void setPayload(Map<String, Object> payload) {
        this.payload = payload;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
