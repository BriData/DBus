package com.creditease.dbus.heartbeat.distributed.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.creditease.dbus.heartbeat.distributed.Cluster;
import com.creditease.dbus.heartbeat.distributed.Worker;

public class DefaultTopicAssignor implements TopicAssignor {

    @Override
    public String name() {
        return "avg rang";
    }

    @Override
    public Map<Worker, Assignment> assign(Cluster cluster, List<String> topics) {
        final Map<Worker, Assignment> assignment = new HashMap<>();
        int avg = topics.size() / cluster.workerCount();
        for (int i=0; i<cluster.workerCount(); i++) {
            int start = avg * i;
            int end = (i == cluster.workerCount() -1) ? topics.size() : avg * (i + 1);
            assignment.put(cluster.getWorkers().get(i), new Assignment(topics.subList(start, end)));
        }
        return assignment;
    }

}
