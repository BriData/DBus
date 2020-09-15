package com.creditease.dbus.heartbeat.distributed.utils;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;

import com.alibaba.fastjson.JSON;
import com.creditease.dbus.heartbeat.container.CuratorContainer;
import com.creditease.dbus.heartbeat.distributed.Cluster;
import com.creditease.dbus.heartbeat.distributed.Worker;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.Watcher;

public class  ZkUtils {

    public String heartBeatRootPath = "/dbus/heartbeat";
    public String leaderPath = heartBeatRootPath + "/leader";
    public String workersIdsPath = heartBeatRootPath + "/workers/ids";

    public String configPath = heartBeatRootPath + "/config";
    public String controlPath = heartBeatRootPath + "/control";
    public String monitorPath = heartBeatRootPath + "/monitor";
    public String projectMonitorPath = heartBeatRootPath + "/project_monitor";

    public String assignmentsPath = heartBeatRootPath + "/assignments";
    public String assignmentsTopicsPath = assignmentsPath + "/topics";

    private List<String> persistentZkPaths = Arrays.asList(heartBeatRootPath,
                                                           leaderPath,
                                                           workersIdsPath,
                                                           configPath,
                                                           controlPath,
                                                           monitorPath,
                                                           projectMonitorPath,
                                                           assignmentsPath,
                                                           assignmentsTopicsPath);

    public void setupCommonPaths() throws Exception {
        for (String path : persistentZkPaths) {
            createPath(path);
        }
    }

    public void createPath(String path) throws Exception {
        CuratorFramework curator = CuratorContainer.getInstance().getCurator();
        boolean isExists = curator.checkExists().forPath(path) == null ? false : true;
        if (!isExists) {
            curator.create().creatingParentsIfNeeded().forPath(path);
        }
    }

    public void setData(String path, byte[] data) {
        CuratorFramework curator = CuratorContainer.getInstance().getCurator();
        try {
            curator.setData().forPath(path, data);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Cluster getCluster() {

        List<String> children = null;
        CuratorFramework curator = CuratorContainer.getInstance().getCurator();
        try {
            children = curator.getChildren().forPath(workersIdsPath);
        } catch (Exception e) {
        }

        List<Worker> workers = new ArrayList<>();
        for (String path : Optional.ofNullable(children).orElseGet(Collections::emptyList)) {
            StringJoiner joiner = new StringJoiner("/");
            joiner.add(workersIdsPath).add(path);
            try {
                String data = new String(curator.getData().forPath(joiner.toString()), Charset.forName("UTF-8"));
                Worker worker = JSON.parseObject(data, Worker.class);
                workers.add(worker);
            } catch (Exception e) {
            }
        }

        Cluster cluster = new Cluster(workers);
        return cluster;
    }

    public void usingWatcher(String path, Watcher watcher) {
        CuratorFramework curator = CuratorContainer.getInstance().getCurator();
        try {
            curator.getData().usingWatcher(watcher).forPath(ZKPaths.makePath(path, null));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void setConnectionStateListenable(ConnectionStateListener listener) {
        CuratorFramework curator = CuratorContainer.getInstance().getCurator();
        curator.getConnectionStateListenable().addListener(listener);
    }



}
