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


package com.creditease.dbus.heartbeat.parsing;

import com.alibaba.fastjson.JSONObject;

/**
 * Created by mal on 2019/5/13.
 */
public abstract class RespParser<T> {

    protected String resp;

    protected JSONObject json;

    protected RespParser(String resp) {
        this.resp = resp;
        json = JSONObject.parseObject(resp);
    }

    public boolean isSuccess() {
        return getStatus() == 0;
    }

    protected int getStatus() {
        return (json == null) ? -1 : json.getInteger("status");
    }

    protected String getMessage() {
        return (json == null) ? "api result is empty" : json.getString("message");
    }

    protected JSONObject payload() {
        return (json == null) ? null : json.getJSONObject("payload");
    }

    public String toHtml() {
        StringBuilder html = new StringBuilder();
        html.append("<table bgcolor=\"#c1c1c1\">");
        html.append("    <tr bgcolor=\"#ffffff\">");
        html.append("        <th>Status</th>");
        html.append("        <th>Message</th>");
        html.append("    </tr>");
        html.append("    <tr bgcolor=\"#ffffff\">");
        html.append("        <th align=\"left\">" + getStatus() + "</th>");
        html.append("        <th align=\"left\">" + getMessage() + "</th>");
        html.append("    </tr>");
        html.append("</table>");
        return html.toString();
    }

    public abstract T parse();

}
