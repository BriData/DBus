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


package com.creditease.dbus.stream.appender.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Shrimp on 16/6/20.
 */
public class ReloadStatus implements Serializable {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private static final long RELOAD_TIME_THRESHOLD = 20;

    private long lastReloadTime;
    private boolean ready2reloading = false;
    private Object param;
    private Map<String, Object> extMap;

    private int reloadTimes = 0;


    public ReloadStatus() {
        lastReloadTime = 0;
        extMap = new HashMap<>();
    }

    public boolean isReadyToReload() {
        //if (ready2reloading) {
        //if (System.currentTimeMillis() - lastReloadTime > RELOAD_TIME_THRESHOLD * 1000) {
        //    return true;
        //}
        //logger.warn("Can't reload too frequently, limitation is {}s", RELOAD_TIME_THRESHOLD);
        //}
        return ready2reloading;
    }

    public void markReloading(Object param) {
        ready2reloading = true;
        this.param = param;
    }

    public void reloaded() {
        this.lastReloadTime = System.currentTimeMillis();
        this.ready2reloading = false;
        reloadTimes++;
    }

    public Object getParameter() {
        return param;
    }

    public void addExtParam(String key, Object val) {
        extMap.put(key, val);
    }

    public void addAllExts(Map<String, Object> map) {
        extMap.putAll(map);
    }

    public <T> T getExtParam(String key) {
        return (T) extMap.get(key);
    }
}
