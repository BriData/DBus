package com.creditease.dbus.heartbeat.distributed.controller;

import com.creditease.dbus.heartbeat.distributed.Worker;
import com.creditease.dbus.heartbeat.distributed.utils.ZkUtils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.ZKPaths;

public class WorkerHealthcheck {

    private ZkUtils zkUtils;
    private Worker worker;

    public WorkerHealthcheck(ZkUtils zkUtils, Worker worker) {
        this.zkUtils = zkUtils;
        this.worker = worker;
    }

    public void startup() throws Exception {
        registerConnectionStateListener();
        register();
    }

    private void register() throws Exception {
        String path = ZKPaths.makePath(zkUtils.workersIdsPath, String.valueOf(worker.getId()));
        zkUtils.createPath(path);
        zkUtils.setData(path, worker.toString().getBytes());
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
                        try {
                            register();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        break;
                    }
                }
            }
        };
        zkUtils.setConnectionStateListenable(listener);
    }

}
