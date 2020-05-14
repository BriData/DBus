package com.creditease.dbus.heartbeat.distributed.controller;

import com.creditease.dbus.heartbeat.distributed.Worker;
import com.creditease.dbus.heartbeat.distributed.utils.ZkUtils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class WorkStateMachine {

    private ZkUtils zkUtils;
    private Worker  worker;

    public WorkStateMachine(ZkUtils zkUtils, Worker worker) {
        this.zkUtils = zkUtils;
        this.worker = worker;
    }

    public void startup() {
        registerConnectionStateListener();
        registerAssignmentListener();
    }

    private void registerAssignmentListener() {
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == Event.EventType.NodeDataChanged) {
                    // 首先停止worker中分布式的任务，然后再重新启动
                }
            }
        };
        zkUtils.usingWatcher(ZKPaths.makePath(zkUtils.assignmentsTopicsPath, String.valueOf(worker.getId())), watcher);
    }

    private void registerConnectionStateListener() {
        ConnectionStateListener listener = new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                switch (newState) {
                    default: {
                        break;
                    }
                    case RECONNECTED: {
                        registerAssignmentListener();
                        break;
                    }
                }
            }
        };
        zkUtils.setConnectionStateListenable(listener);
    }

}
