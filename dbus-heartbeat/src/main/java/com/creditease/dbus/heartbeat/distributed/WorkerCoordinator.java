package com.creditease.dbus.heartbeat.distributed;

import java.util.Map;

import com.creditease.dbus.heartbeat.distributed.controller.DefaultTopicAssignor;
import com.creditease.dbus.heartbeat.distributed.controller.TopicAssignor;
import com.creditease.dbus.heartbeat.distributed.utils.ZkUtils;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class WorkerCoordinator {

    private ZkUtils zkUtils;
    private TopicAssignor topicAssignor = new DefaultTopicAssignor();

    public WorkerCoordinator(ZkUtils zkUtils) {
        this.zkUtils = zkUtils;
    }

    public void registerWorkerChangeListener() {
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == Event.EventType.NodeChildrenChanged) {
                    Map<Worker, TopicAssignor.Assignment> assignment = topicAssignor.assign(zkUtils.getCluster(), null);
                    // 将leader分配的结果写回zk
                }
            }
        };
        zkUtils.usingWatcher(zkUtils.workersIdsPath, watcher);
    }

}
