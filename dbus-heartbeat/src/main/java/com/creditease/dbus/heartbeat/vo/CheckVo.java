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


package com.creditease.dbus.heartbeat.vo;

import com.creditease.dbus.heartbeat.event.AlarmType;

import java.io.Serializable;
import java.text.MessageFormat;

public class CheckVo implements Serializable {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 8363017883466315154L;

    private AlarmType type;

    private boolean isNormal;

    private String path;

    private int alarmCnt;

    private int timeoutCnt;

    public long getLastAlarmTime() {
        return lastAlarmTime;
    }

    public void setLastAlarmTime(long lastAlarmTime) {
        this.lastAlarmTime = lastAlarmTime;
    }

    private long lastAlarmTime;

    public AlarmType getType() {
        return type;
    }

    public void setType(AlarmType type) {
        this.type = type;
    }

    public boolean isNormal() {
        return isNormal;
    }

    public void setNormal(boolean isNormal) {
        this.isNormal = isNormal;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public int getAlarmCnt() {
        return alarmCnt;
    }

    public void setAlarmCnt(int alarmCnt) {
        this.alarmCnt = alarmCnt;
    }

    public int getTimeoutCnt() {
        return timeoutCnt;
    }

    public void setTimeoutCnt(int timeoutCnt) {
        this.timeoutCnt = timeoutCnt;
    }

    public String html(String diffVal) {
        StringBuilder html = new StringBuilder();
        html.append("<tr bgcolor=\"#ffffff\">");
        html.append("    <th align=\"left\">" + getPath() + "</th>");
        html.append("    <th align=\"right\">" + getAlarmCnt() + "</th>");
        html.append("    <th align=\"right\">" + diffVal + "</th>");
        html.append("    <th align=\"right\">" + getTimeoutCnt() + "</th>");
        html.append("</tr>");
        return html.toString();
    }

    @Override
    public String toString() {
        return MessageFormat.format("节点:{0},状态:{1},报警次数:{2},超时次数:{3}",
                getPath(), (isNormal() ? "正常" : "异常"), getAlarmCnt(), getTimeoutCnt());
    }

}
