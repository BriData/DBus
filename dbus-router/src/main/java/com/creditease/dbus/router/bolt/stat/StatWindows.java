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


package com.creditease.dbus.router.bolt.stat;

import com.creditease.dbus.router.bean.Stat;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2018/6/4.
 */
public class StatWindows {

    private boolean isUseBarrier = false;

    private Map<String, Stat> statMap = new HashMap<>();

    public StatWindows() {
        this(false);
    }

    public StatWindows(boolean isUseBarrier) {
        this.isUseBarrier = isUseBarrier;
    }

    public void add(String namespace, Stat vo) {
        if (statMap.containsKey(namespace)) {
            statMap.get(namespace).merge(vo, isUseBarrier);
        } else {
            statMap.put(namespace, vo);
        }
    }

    /**
     * 当encode bolt 发生错误时，把已经累加的值进行修正
     *
     * @param namespace
     * @param size
     */
    public void correc(String namespace, Integer size) {
        if (statMap.containsKey(namespace))
            statMap.get(namespace).correc(size);
    }

    public Stat poll(String namespace) {
        Stat vo = null;
        if (statMap.containsKey(namespace)) {
            vo = statMap.get(namespace);
            statMap.remove(namespace);
        }
        return vo;
    }

    public Stat tryPoll(String namespace, Integer taskIdSum) {
        Stat vo = statMap.get(namespace);
        if (vo != null && vo.getTaskIdSum() >= taskIdSum) {
            //说明所有encode bolt的stat都已到齐,可以清楚缓存中的数据
            statMap.remove(namespace);
        } else if (vo != null) {
            // 说明所有encode bolt的stat还没有到齐,需要清空vo继续等待
            vo = null;
        }
        return vo;
    }

    public void clear() {
        statMap.clear();
    }
}
