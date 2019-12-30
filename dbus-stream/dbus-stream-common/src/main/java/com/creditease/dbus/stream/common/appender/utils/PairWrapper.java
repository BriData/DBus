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


package com.creditease.dbus.stream.common.appender.utils;

import com.alibaba.fastjson.JSON;

import java.io.Serializable;
import java.util.*;

/**
 * 键值对存储结构的包装器
 * <p>将无顺序要求或者需要使用map特性的key-value存储在map中,
 * 将有顺序要求的键值对存储在Pair数组中</p>
 * Created by Shrimp on 16/5/23.
 */
public class PairWrapper<K, V> implements Serializable {
    private Map<K, V> map;
    private Map<K, Integer> index;
    private List<Pair<K, V>> pairs;
    private Set<K> missingFields;

    public PairWrapper() {
        map = new HashMap<>();
        index = new HashMap<>();
        pairs = new ArrayList<>();
        missingFields = new HashSet<>();
    }

    public void addProperties(K key, V value) {
        map.put(key, value);
    }

    public V getProperties(K key) {
        return map.get(key);
    }

    public V removeProperties(K key) {
        return map.remove(key);
    }

    public Set<K> getMissingFields() {
        return missingFields;
    }

    public void addMissingField(K field) {
        missingFields.add(field);
    }

    public void addPair(Pair<K, V> pair) {
        index.put(pair.getKey(), pairs.size());
        pairs.add(pair);
    }

    public Pair<K, V>[] pairs2array() {
        Pair<K, V>[] arr = new Pair[0];
        return pairs.toArray(arr);
    }

    public List<Pair<K, V>> getPairs() {
        return pairs;
    }

    public Map<K, V> getMap() {
        return map;
    }

    public void setMap(Map<K, V> map) {
        this.map = map;
    }

    public void setPairs(List<Pair<K, V>> pairs) {
        this.pairs = pairs;
    }

    public void setMissingFields(Set<K> missingFields) {
        this.missingFields = missingFields;
    }

    public Map<K, Integer> getIndex() {
        return index;
    }

    public void setIndex(Map<K, Integer> index) {
        this.index = index;
    }

    public void removePairsByKey(K key) {
        Integer index = this.index.get(key);
        pairs.remove(index);
    }

    public Pair<K, V> getPair(K key) {
        if (!index.containsKey(key)) return null;
        return pairs.get(index.get(key));
    }

    public V getPairValue(K key) {
        Pair<K, V> p = getPair(key);
        if (p != null) {
            return p.getValue();
        }
        return null;
    }

    public Map<K, V> pairs2map() {
        Map<K, V> map = new HashMap<>();
        for (Pair<K, V> pair : pairs) {
            map.put(pair.getKey(), pair.getValue());
        }
        return map;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
