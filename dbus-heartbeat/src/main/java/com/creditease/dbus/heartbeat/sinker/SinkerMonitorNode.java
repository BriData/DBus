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


package com.creditease.dbus.heartbeat.sinker;

public class SinkerMonitorNode {
    //报警次数
    private int alarmCount;
    //超时次数
    private int timeoutCnt;
    //上次报警时间
    private long lastAlarmTime;
    //sinker心跳延时时间
    private int latencyMS;
    //该节点心跳延时更新时间
    private long updateTime;
    //实际延时时间
    private long realLatencyMS;

    public SinkerMonitorNode() {
        this.alarmCount = 0;
        this.timeoutCnt = 0;
        this.lastAlarmTime = 0;
        this.latencyMS = 0;
    }

    public int getAlarmCount() {
        return alarmCount;
    }

    public void setAlarmCount(int alarmCount) {
        this.alarmCount = alarmCount;
    }

    public int getTimeoutCnt() {
        return timeoutCnt;
    }

    public void setTimeoutCnt(int timeoutCnt) {
        this.timeoutCnt = timeoutCnt;
    }

    public long getLastAlarmTime() {
        return lastAlarmTime;
    }

    public void setLastAlarmTime(long lastAlarmTime) {
        this.lastAlarmTime = lastAlarmTime;
    }

    public int getLatencyMS() {
        return latencyMS;
    }

    public boolean isRunning() {
        return updateTime != 0;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public void setLatencyMS(int latencyMS) {
        this.latencyMS = latencyMS;
        this.updateTime = System.currentTimeMillis();
    }

    public long getRealLatencyMS() {
        return realLatencyMS;
    }

    public void setRealLatencyMS(long realLatencyMS) {
        this.realLatencyMS = realLatencyMS;
    }
}
