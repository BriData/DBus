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


package com.creditease.dbus.stream.db2.appender.bolt.processor.vo;

import org.apache.avro.generic.GenericRecord;

/**
 * @author zhenlinzhong
 * @date 2018/11/6 15:42
 */
public class AppenderDataResults {
    private GenericRecord genericRecord;
    private String offset;

    public GenericRecord getGenericRecord() {
        return genericRecord;
    }

    public void setGenericRecord(GenericRecord genericRecord) {
        this.genericRecord = genericRecord;
    }

    public String getOffset() {
        return offset;
    }

    public void setOffset(String offset) {
        this.offset = offset;
    }
}
