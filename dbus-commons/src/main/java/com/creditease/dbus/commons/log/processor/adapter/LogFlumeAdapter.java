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


package com.creditease.dbus.commons.log.processor.adapter;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class LogFlumeAdapter implements Iterator<String> {
    private static Logger logger = LoggerFactory.getLogger(LogFlumeAdapter.class);

    private String key;
    private String value;

    private Iterator<String> it;

    public LogFlumeAdapter(String key, String value) {
        this.key = key;
        this.value = value;
        adapt();
    }

    private void adapt() {
        List<String> wk = new ArrayList<>();
        Map<String, String> data = null;
        data = JSON.parseObject(value, HashMap.class);
        data.put("timestamp", key);
        wk.add(JSON.toJSONString(data));
        it = wk.iterator();
    }

    public boolean hasNext() {
        return it.hasNext();
    }

    public String next() {
        return it.next();
    }

}
