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


package com.creditease.dbus.router.encode;

import com.creditease.dbus.msgencoder.EncodeColumn;

/**
 * Created by Administrator on 2018/7/23.
 */
public class DBusRouterEncodeColumn extends EncodeColumn {

    private String fieldType;
    private int schemaChangeFlag;
    private long tptteocId;
    private long tpttId;
    private int precision;
    private int scale;
    private boolean isCanEncode = false;
    private boolean isChanged = false;
    private boolean isDelete = false;
    private String schemaChangeComment;

    public int getSchemaChangeFlag() {
        return schemaChangeFlag;
    }

    public void setSchemaChangeFlag(int schemaChangeFlag) {
        this.schemaChangeFlag = schemaChangeFlag;
    }

    public long getTptteocId() {
        return tptteocId;
    }

    public void setTptteocId(long tptteocId) {
        this.tptteocId = tptteocId;
    }

    public long getTpttId() {
        return tpttId;
    }

    public void setTpttId(long tpttId) {
        this.tpttId = tpttId;
    }

    public String getFieldType() {
        return fieldType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    public int getScale() {
        return scale;
    }

    public void setScale(int scale) {
        this.scale = scale;
    }

    public boolean isCanEncode() {
        return isCanEncode;
    }

    public void setCanEncode(boolean canEncode) {
        isCanEncode = canEncode;
    }

    public boolean isChanged() {
        return isChanged;
    }

    public void setChanged(boolean changed) {
        isChanged = changed;
    }

    public String getSchemaChangeComment() {
        return schemaChangeComment;
    }

    public void setSchemaChangeComment(String schemaChangeComment) {
        this.schemaChangeComment = schemaChangeComment;
    }

    public boolean isDelete() {
        return isDelete;
    }

    public void setDelete(boolean delete) {
        isDelete = delete;
    }
}
