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


package com.creditease.dbus.router.bean;

import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;
import java.util.*;

/**
 * Created by Administrator on 2018/6/28.
 */
public class ReadSpoutConfig implements Serializable {

    private Set<String> topics = new HashSet<>();

    private Set<String> namespaces = new TreeSet<>();

    private Map<String, Long> namespaceTableIdPair = new HashMap<>();

    private Map<String, TopicPartition> topicPartitionMap = new HashMap<>();

    private Map<String, List<TopicPartition>> topicMap = new HashMap();

    public Set<String> getTopics() {
        return topics;
    }

    public void setTopics(Set<String> topics) {
        this.topics = topics;
    }

    public Set<String> getNamespaces() {
        return namespaces;
    }

    public void setNamespaces(Set<String> namespaces) {
        this.namespaces = namespaces;
    }

    public Map<String, Long> getNamespaceTableIdPair() {
        return namespaceTableIdPair;
    }

    public void setNamespaceTableIdPair(Map<String, Long> namespaceTableIdPair) {
        this.namespaceTableIdPair = namespaceTableIdPair;
    }

    public Map<String, TopicPartition> getTopicPartitionMap() {
        return topicPartitionMap;
    }

    public void setTopicPartitionMap(Map<String, TopicPartition> topicPartitionMap) {
        this.topicPartitionMap = topicPartitionMap;
    }

    public Map<String, List<TopicPartition>> getTopicMap() {
        return topicMap;
    }

    public void setTopicMap(Map<String, List<TopicPartition>> topicMap) {
        this.topicMap = topicMap;
    }

}
