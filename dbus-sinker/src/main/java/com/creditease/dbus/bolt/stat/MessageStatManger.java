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


package com.creditease.dbus.bolt.stat;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class MessageStatManger {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private Map<String, Stat> statMap = null;

    public MessageStatManger() {
        this.statMap = new HashMap<>();
    }

    public Stat get(String key) {
        return statMap.get(key);
    }

    //数据处理成功
    public void put(String key, String dataKey, Integer cnt) {
        String[] dataKeys = StringUtils.split(dataKey, ".");
        Stat stat = get(key);
        if (stat == null) {
            stat = new Stat(dataKeys[2], dataKeys[3], dataKeys[4]);
        }
        stat.commitDataCount(cnt);
        statMap.put(key, stat);
    }

    //数据处理失败
    public void remove(String key, int cnt) {
        Stat stat = get(key);
        if (stat != null) {
            stat.failDataCount(cnt);
        }
    }

}
