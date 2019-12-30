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


package com.creditease.dbus.heartbeat.container;

import com.creditease.dbus.heartbeat.log.LoggerFactory;
import com.creditease.dbus.heartbeat.vo.ZkVo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;

public class CuratorContainer {

    private static CuratorContainer container;

    private CuratorFramework curator;

    private CuratorContainer() {
    }

    public static CuratorContainer getInstance() {
        if (container == null) {
            synchronized (CuratorContainer.class) {
                if (container == null)
                    container = new CuratorContainer();
            }
        }
        return container;
    }

    public boolean register(ZkVo conf) {
        boolean isOk = true;
        try {
            curator = CuratorFrameworkFactory.newClient(
                    conf.getZkStr(),
                    conf.getZkSessionTimeout(),
                    conf.getZkConnectionTimeout(),
                    new RetryNTimes(Integer.MAX_VALUE, conf.getZkRetryInterval()));
            curator.start();
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[connection zk path:" + conf.getZkStr() + " error!]", e);
            isOk = false;
        }
        return isOk;
    }

    public CuratorFramework getCurator() {
        return curator;
    }

    public boolean createZkNode(String path) {
        boolean isExists = false;
        try {
            LoggerFactory.getLogger().info("path: " + path);
            isExists = curator.checkExists().forPath(path) == null ? false : true;
            if (!isExists) {
                curator.create().creatingParentsIfNeeded().forPath(path, null);
                LoggerFactory.getLogger().info("[create-znode] 创建znode: " + path + ".");
            } else {
                LoggerFactory.getLogger().info("[create-znode] znode: " + path + "已经存在.");
            }
        } catch (Exception e) {
            throw new RuntimeException("[create-znode] 检查znode: " + path + "出错.");
        }
        return isExists;
    }

    public void close() {
        curator.close();
    }
}
