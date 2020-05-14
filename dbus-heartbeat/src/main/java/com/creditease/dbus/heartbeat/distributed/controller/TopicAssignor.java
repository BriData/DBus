package com.creditease.dbus.heartbeat.distributed.controller;

import java.util.List;
import java.util.Map;

import com.creditease.dbus.heartbeat.distributed.Cluster;
import com.creditease.dbus.heartbeat.distributed.Worker;

public interface TopicAssignor {

    String name();

    Map<Worker, Assignment> assign(Cluster cluster, List<String> topics);

    class Assignment {
        private List<String> topics;

        public Assignment(List<String> topics) {
            this.topics = topics;
        }
        public boolean add(String topic) {
            return topics.add(topic);
        }
        public List<String> getTopics() {
            return topics;
        }
    }

}
