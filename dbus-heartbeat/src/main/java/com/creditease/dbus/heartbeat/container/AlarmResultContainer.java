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


package com.creditease.dbus.heartbeat.container;

import com.creditease.dbus.heartbeat.vo.CheckVo;

import java.util.concurrent.ConcurrentHashMap;

public class AlarmResultContainer {

    private static AlarmResultContainer container;

    private ConcurrentHashMap<String, CheckVo> chbmp = new ConcurrentHashMap<String, CheckVo>();

    // private ConcurrentHashMap<String, CheckFullPullVo> cfpmp = new ConcurrentHashMap<String, CheckFullPullVo>();

    private static StringBuilder html = new StringBuilder();

    static {
        html.append("<table bgcolor=\"#c1c1c1\">");
        html.append("    <tr bgcolor=\"#ffffff\">");
        html.append("        <th>Zk Path</th>");
        html.append("        <th>Alarm Count</th>");
        html.append("        <th>Time Out</th>");
        html.append("        <th>Time Out Count</th>");
        html.append("    </tr>");
        html.append("{0}");
        html.append("</table>");
    }

    private AlarmResultContainer() {
    }

    public static AlarmResultContainer getInstance() {
        if (container == null) {
            synchronized (AlarmResultContainer.class) {
                if (container == null)
                    container = new AlarmResultContainer();
            }
        }
        return container;
    }

    public void put(String key, CheckVo value) {
        chbmp.put(key, value);
    }

    public CheckVo get(String key) {
        return chbmp.get(key);
    }

    public String html() {
        return html.toString();
    }

    public void clear() {
        chbmp.clear();
        // cfpmp.clear();
    }

}
