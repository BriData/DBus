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


package com.creditease.dbus.encoders;

import java.util.Map;

/**
 * Created by zhangyf on 17/6/1.
 */
public class EncoderConf {
    private String fieldName;
    private String fieldType;
    private int length;
    private String encodeType;
    private String encodeParam;
    private boolean truncate;
    private String desc;
    /**
     * dbus message 中原始数据
     */
    private Map<String, Object> raw;

    public EncoderConf() {
    }

    public EncoderConf(String fieldName, String fieldType, int length,
                       String encodeType, String encodeParam, boolean truncate,
                       Map<String, Object> raw, String desc) {
        this.fieldName = fieldName;
        this.length = length;
        this.encodeType = encodeType;
        this.encodeParam = encodeParam;
        this.truncate = truncate;
        this.desc = desc;
        this.fieldType = fieldType;
        this.raw = raw;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public String getEncodeType() {
        return encodeType;
    }

    public void setEncodeType(String encodeType) {
        this.encodeType = encodeType;
    }

    public String getEncodeParam() {
        return encodeParam;
    }

    public void setEncodeParam(String encodeParam) {
        this.encodeParam = encodeParam;
    }

    public boolean isTruncate() {
        return truncate;
    }

    public void setTruncate(boolean truncate) {
        this.truncate = truncate;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public String getFieldType() {
        return fieldType;
    }

    public Map<String, Object> getRaw() {
        return raw;
    }

    public void setRaw(Map<String, Object> raw) {
        this.raw = raw;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }
}
