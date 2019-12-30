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


package com.creditease.dbus.stream.common.appender.bean;

import com.alibaba.fastjson.JSON;
import com.creditease.dbus.commons.MetaWrapper;

import java.io.Serializable;
import java.util.Map;

/**
 * Meta版本数据结构
 * Created by Shrimp on 16/5/23.
 */
public class MetaVersion implements Serializable {

    private long id;
    private long dsId;
    private long tableId;
    private String schema;
    private String table;
    private long offset;
    private long trailPos;
    private int version; // 外部版本号
    private int innerVersion;
    private Integer schemaHash; // 该版本数据对应的schema哈希值
    private String comments;
    private Integer abandon; // 假删除标记,0或null表示未删除,1表示删除

    private MetaWrapper meta;
    
    public MetaVersion(){
        this(0, 0);
    }

    public MetaVersion(int v, int iv) {
        version = v;
        innerVersion = iv;
        this.setOffset(0L);
        this.setTrailPos(0L);
    }

    public static MetaVersion parse(Map<String, Object> data) {
        MetaVersion v = new MetaVersion();
        v.setId((Long)data.get("id"));
        v.setDsId((Long) data.get("ds_id"));
        v.setSchema(data.get("schema_name").toString());
        v.setTable(data.get("table_name").toString());
        v.setVersion((Integer)data.get("version"));
        v.setInnerVersion((Integer)data.get("inner_version"));
        v.setSchemaHash((Integer) data.get("schema_hash"));
        v.setOffset((Long)data.get("event_offset"));
        v.setTrailPos((Long)data.get("event_pos"));
        v.setTableId((Integer)data.get("table_id"));
        v.setComments((String)data.get("comments"));
        return v;
    }

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public MetaWrapper getMeta() {
        return meta;
    }

    public void setMeta(MetaWrapper meta) {
        this.meta = meta;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getDsId() {
        return dsId;
    }

    public void setDsId(long dsId) {
        this.dsId = dsId;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getTrailPos() {
        return trailPos;
    }

    public void setTrailPos(long trailPos) {
        this.trailPos = trailPos;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public void setInnerVersion(int innerVersion) {
        this.innerVersion = innerVersion;
    }

    public int nextVer() {
       return version++;
    }
    public int nextInnerVer() {
        return innerVersion++;
    }
    public int getVersion() {
        return version;
    }

    public int getInnerVersion() {
        return innerVersion;
    }

    public Integer getSchemaHash() {
        return schemaHash;
    }

    public void setSchemaHash(Integer schemaHash) {
        this.schemaHash = schemaHash;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    public Integer getAbandon() {
        return abandon;
    }

    public void setAbandon(Integer abandon) {
        this.abandon = abandon;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

}
