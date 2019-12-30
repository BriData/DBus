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


package com.creditease.dbus.stream.common.appender.bolt.processor.stat;

import com.alibaba.fastjson.JSON;

/**
 * Created by dongwang47 on 2016/8/23.
 */
public class HeartBeatPacket {
    private String type;
    private String node;
    private long time;
    private long txtime;

    public static HeartBeatPacket parse(String jsonString) {
        return JSON.parseObject(jsonString, HeartBeatPacket.class);
    }


    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getNode() {
        return node;
    }

    public void setNode(String node) {
        this.node = node;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long getTxtime() {
        return txtime;
    }

    public void setTxtime(long txtime) {
        this.txtime = txtime;
    }
}
