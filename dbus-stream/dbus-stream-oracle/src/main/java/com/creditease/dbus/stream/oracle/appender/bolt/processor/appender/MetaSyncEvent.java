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


package com.creditease.dbus.stream.oracle.appender.bolt.processor.appender;

import com.creditease.dbus.stream.common.appender.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Created by Shrimp on 16/5/27.
 */
public class MetaSyncEvent implements Serializable {
    private static Logger logger = LoggerFactory.getLogger(MetaSyncEvent.class);
    private long serno;
    private String schema;
    private String tableName;
    private Integer version;
    private String ddlType;
    private Timestamp eventTime;
    private String ddl;

    public static MetaSyncEvent build(Map<String, Object> data) {
        MetaSyncEvent e = new MetaSyncEvent();
        e.setSerno(Long.parseLong(data.get("SERNO").toString()));
        e.setSchema(data.get("TABLE_OWNER").toString());
        e.setTableName(data.get("TABLE_NAME").toString());
        e.setVersion(Double.valueOf(data.get("VERSION").toString()).intValue());
        e.setDdlType(data.get("DDL_TYPE").toString());
        try {
            Date date = Utils.isoDateTimeDdate(data.get("EVENT_TIME").toString());
            e.setEventTime(new Timestamp(date.getTime()));
        } catch (Exception e1) {
            logger.warn("MetaSyncEvent event time convert error", e);
        }

        Object ddl = data.get("DDL");
        if (ddl != null) {
            e.setDdl(ddl.toString());
        }
        return e;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public String getDdlType() {
        return ddlType;
    }

    public void setDdlType(String ddlType) {
        this.ddlType = ddlType;
    }

    public Timestamp getEventTime() {
        return eventTime;
    }

    public void setEventTime(Timestamp eventTime) {
        this.eventTime = eventTime;
    }

    public String getDdl() {
        return ddl;
    }

    public void setDdl(String ddl) {
        this.ddl = ddl;
    }

    public long getSerno() {
        return serno;
    }

    public void setSerno(long serno) {
        this.serno = serno;
    }

    @Override
    public String toString() {
        return "MetaSyncEvent={" +
                "schema='" + schema + '\'' +
                ", tableName='" + tableName + '\'' +
                ", version=" + version +
                ", ddlType='" + ddlType + '\'' +
                ", eventTime=" + eventTime +
                ", ddl='" + ddl + '\'' +
                '}';
    }

    public static void main(String[] args) throws Exception {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss.SSS");
        System.out.println(df.parse("2011-11-11:12:12:12.123"));
        Date d = df.parse("2011-11-11:12:12:12.123");
        df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        System.out.println(df.format(d));
    }
}
