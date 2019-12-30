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


package com.creditease.dbus.stream.appender.spout.queue;

import com.creditease.dbus.commons.DBusConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhangyf on 17/2/28.
 */
public class QueueElement {
    private Logger logger = LoggerFactory.getLogger(getClass());
    public static final int INIT = 0;
    public static final int OK = 1;
    public static final int FAIL = -1;

    private int status = INIT;
    private long key = -1L;
    private int emitCount = 0;
    private DBusConsumerRecord<String, byte[]> record;

    public QueueElement(DBusConsumerRecord<String, byte[]> record) {
        this.key = record.offset();
        setRecord(record);
    }

    public void fail() {
        if (!isOk()) {
            this.status = FAIL;
            logger.info("message[topic:{},partition:{},offset:{},emitcount:{}] status was set to {}.",
                    record.topic(), record.partition(), record.offset(), this.emitCount, "'fail'");
        } else {
            logger.warn("skip to set fail. message[topic:{},partition:{},offset:{},emitCount:{},status{}].",
                    record.topic(), record.partition(), record.offset(), this.emitCount, status);
        }
        minusEmitCount();
    }

    public void ok() {
        this.status = OK;
        minusEmitCount();
    }

    public boolean isFailed() {
        return status == FAIL;
    }

    public boolean isOk() {
        return status == OK;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public long getKey() {
        return key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public DBusConsumerRecord<String, byte[]> getRecord() {
        return record;
    }

    public int getEmitCount() {
        return emitCount;
    }

    public void setRecord(DBusConsumerRecord<String, byte[]> record) {
        this.record = record;
        emitCount++;
    }

    private void minusEmitCount() {
        emitCount--;
        if (emitCount < 0) {
            emitCount = 0;
            logger.error("Impossible!!! key:{},status:{},record:[{}.{}]", key, status, record.topic(), record.partition());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QueueElement)) return false;

        QueueElement that = (QueueElement) o;

        if (key != that.key) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return (int) (key ^ (key >>> 32));
    }
}
