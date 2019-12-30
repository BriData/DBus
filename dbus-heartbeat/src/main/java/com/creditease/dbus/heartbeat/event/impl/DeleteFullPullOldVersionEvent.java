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


package com.creditease.dbus.heartbeat.event.impl;

import com.creditease.dbus.heartbeat.container.CuratorContainer;
import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.event.AbstractEvent;
import com.creditease.dbus.heartbeat.log.LoggerFactory;
import com.creditease.dbus.heartbeat.vo.HeartBeatVo;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * @author Liang.Ma
 * @version 1.0
 */
public class DeleteFullPullOldVersionEvent extends AbstractEvent {

    private Lock lock;

    public DeleteFullPullOldVersionEvent(long interval, Lock lock) {
        super(interval);
        this.lock = lock;
    }

    @Override
    public void run() {
        CuratorFramework curator = CuratorContainer.getInstance().getCurator();
        HeartBeatVo hbConf = HeartBeatConfigContainer.getInstance().getHbConf();
        int fullPullOldVersionCnt = hbConf.getFullPullOldVersionCnt();
        while (isRun.get()) {
            lock.lock();
            try {
                Map<String, List<String>> map = new HashMap<String, List<String>>();
                List<String> list = new ArrayList<String>();
                getChildrenPath(curator, list, hbConf.getMonitorFullPullPath());
                //     List<String> list = curator.getChildren().forPath(hbConf.getMonitorFullPullPaht());
                if (CollectionUtils.isNotEmpty(list)) {
                    for (String item : list) {
                        String key = StringUtils.substringBeforeLast(item, "/");
                        if (!map.containsKey(key)) {
                            List<String> wkList = new ArrayList<String>();
                            map.put(key, wkList);
                        }
                        //跳过version为null的节点
                        String version = StringUtils.substringAfterLast(item, "/");
                        if (!version.equalsIgnoreCase("null")) {
                            map.get(key).add(item);
                        }
                    }
                    for (Map.Entry<String, List<String>> entry : map.entrySet()) {
                        List<String> wkList = entry.getValue();
                        if (wkList.size() < fullPullOldVersionCnt)
                            continue;
                        Collections.sort(entry.getValue(), new VersionComparator());
                        for (int i = fullPullOldVersionCnt; i < wkList.size(); i++) {
                            LoggerFactory.getLogger().info("delete full pull old version: {}", wkList.get(i));
                            //   String path = StringUtils.join(new String[] {hbConf.getMonitorFullPullPaht(), wkList.get(i)},  "/");
                            String path = wkList.get(i);
                            curator.delete().forPath(path);
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("[delete-fullpull-old-version-event]", e);
            } finally {
                lock.unlock();
            }
            sleep(interval, TimeUnit.HOURS);
        }
    }

    private void getChildrenPath(CuratorFramework curator, List<String> lst, String parent) throws Exception {
        List<String> lstPath = curator.getChildren().forPath(parent);
        for (String path : lstPath) {
            getChildrenPath(curator, lst, parent + "/" + path);
        }
        if (lstPath.size() == 0)
            lst.add(parent);
    }

    private class VersionComparator implements Comparator<String> {
        @Override
        public int compare(String str1, String str2) {
            String v1 = StringUtils.substringAfterLast(str1, "/");
            String v2 = StringUtils.substringAfterLast(str2, "/");
            if (v1.compareTo(v2) > 0)
                return -1;
            else if (v1.compareTo(v2) < 0)
                return 1;
            return 0;
        }
    }
}
