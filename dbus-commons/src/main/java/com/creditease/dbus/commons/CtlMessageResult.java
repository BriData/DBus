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


package com.creditease.dbus.commons;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.serializer.SerializerFeature;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Map;

/**
 * Created by zhangyf on 16/12/22.
 */
public class CtlMessageResult {
    private String componentName;
    private String runningHost;
    @JSONField(format = "yyyy-MM-dd HH:mm:ss.SSS")
    private Date opts;
    private Map<String, Object> result;
    private String message;
    @JSONField(serialize = false)
    private ControlMessage originalMessage;

    public CtlMessageResult(String componentName, String message) {
        this.componentName = componentName;
        this.message = message;
        this.runningHost = getLocalIP();
        this.opts = new Date();
    }

    private String getLocalIP() {
        try {
            String hostName = InetAddress.getLocalHost().getHostName();
            InetAddress[] addresses = InetAddress.getAllByName(hostName);
            String ip = "";
            for (InetAddress address : addresses) {
                ip += address.getHostAddress() + ",";
            }
            return ip.endsWith(",") ? ip.substring(0, ip.length() - 1) : ip;
        } catch (UnknownHostException e) {
            return null;
        }
    }

    public ControlMessage getOriginalMessage() {
        return originalMessage;
    }

    public void setOriginalMessage(ControlMessage originalMessage) {
        this.originalMessage = originalMessage;
    }

    public String getComponentName() {
        return componentName;
    }

    public void setComponentName(String componentName) {
        this.componentName = componentName;
    }

    public String getRunningHost() {
        return runningHost;
    }

    public void setRunningHost(String runningHost) {
        this.runningHost = runningHost;
    }

    public Date getOpts() {
        return opts;
    }

    public void setOpts(Date opts) {
        this.opts = opts;
    }

    public Map<String, Object> getResult() {
        return result;
    }

    public void setResult(Map<String, Object> result) {
        this.result = result;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        // 生成格式化的json
        return JSON.toJSONString(this, SerializerFeature.PrettyFormat);
    }

    public static void main(String[] args) {
        System.out.println(new CtlMessageResult("x", "reload finish").toString());
    }
}
