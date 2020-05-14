package com.creditease.dbus.heartbeat.distributed.controller;

import com.creditease.dbus.heartbeat.container.CuratorContainer;
import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.distributed.WorkerCoordinator;
import com.creditease.dbus.heartbeat.distributed.utils.ZkUtils;

import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;

public class HeartBeatController {

    private boolean isLeader = false;
    private LeaderLatch leaderLatch = null;
    private ZkUtils zkUtils;

    public HeartBeatController(ZkUtils zkUtils) {
        this.zkUtils = zkUtils;
        leaderLatch = new LeaderLatch(CuratorContainer.getInstance().getCurator(), HeartBeatConfigContainer.getInstance().getHbConf().getLeaderPath());
    }

    public void start() throws Exception {
        leaderLatch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                isLeader = false;
                WorkerCoordinator coordinator = new WorkerCoordinator(zkUtils);
                coordinator.registerWorkerChangeListener();
            }
            @Override
            public void notLeader() {
            }
        });
        leaderLatch.start();
    }

    public boolean isLeader() {
        return isLeader;
    }
}
