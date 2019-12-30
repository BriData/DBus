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


package com.creditease.dbus.stream.appender.kafka;

import com.alibaba.fastjson.annotation.JSONField;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Created by Shrimp on 16/7/20.
 */
public class TopicInfo {
    private static Logger logger = LoggerFactory.getLogger(TopicInfo.class);
    private String topic;
    private int partition;
    private long offset;
    private Object parameter;
    @JSONField(format = "yyyy-MM-dd HH:mm:ss.SSS")
    private Date time;

    public static TopicInfo build(Map<String, Object> data) {
        TopicInfo bean = new TopicInfo();
        bean.setTopic(data.get("topic").toString());
        bean.setPartition((Integer) data.get("partition"));
        bean.setOffset(Long.parseLong(data.get("offset").toString()));
        bean.setParameter(data.get("parameter"));
        Object date = data.get("time");
        if (date != null) {
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            try {
                bean.setTime(df.parse(date.toString()));
            } catch (ParseException e) {
                logger.error("Date convert error.{}", date);
            }
        }
        return bean;
    }

    public static TopicInfo build(String topic, int partition, long offset, Object parameter) {
        TopicInfo bean = new TopicInfo();
        bean.setTopic(topic);
        bean.setPartition(partition);
        bean.setOffset(offset);
        bean.setParameter(parameter);
        bean.setTime(new Date());
        return bean;
    }

    public TopicPartition partition() {
        return new TopicPartition(getTopic(), getPartition());
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public Object getParameter() {
        return parameter;
    }

    public void setParameter(Object parameter) {
        this.parameter = parameter;
    }

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }
}
