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


package com.creditease.dbus.spout.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShardElement {
    private Logger logger = LoggerFactory.getLogger(getClass());

    public static final int INIT = 0;
    public static final int OK = 1;
    public static final int FAIL = -1;

    private int emitCount;
    private long offset;
    private long index;
    private int status;

    public ShardElement(long offset, long index) {
        this.offset = offset;
        this.index = index;
        this.status = INIT;
        this.emitCount = 1;
    }

    public void fail() {
        if (!isOk()) {
            this.status = FAIL;
            logger.info("message[offset:{},index:{}] status was set to {}.", offset, index, "'fail'");
        } else {
            logger.warn("skip to set fail. message[offset:{},index:{},status{}].", offset, index, status);
        }
    }

    public void emit() {
        this.status = INIT;
        this.emitCount++;
    }

    /**
     * 一个分片最多处理三次,三次还不成功fail
     */
    public boolean canEmit() {
        if (!isOk() && emitCount < 3) {
            return true;
        }
        return false;
    }

    public void ok() {
        this.status = OK;
    }

    public boolean isFailed() {
        return status == FAIL;
    }

    public boolean isOk() {
        return status == OK;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public long getIndex() {
        return index;
    }

    public long getOffset() {
        return offset;
    }

}
