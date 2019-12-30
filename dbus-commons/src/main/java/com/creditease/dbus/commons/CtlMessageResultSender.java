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


package com.creditease.dbus.commons;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhangyf on 16/12/22.
 */
public class CtlMessageResultSender {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private static final String UTF8 = "utf-8";
    private static final String CTL_MST_RESULT_BASE = Constants.DBUS_ROOT + "/ControlMessageResult";
    private ZkService zk;
    private String parentNode;

    public CtlMessageResultSender(String parentNode, String zkconnect) {
        this.zk = buildZKService(zkconnect);
        this.parentNode = parentNode;
        try {
            if (!zk.isExists(CTL_MST_RESULT_BASE)) {
                zk.createNode(CTL_MST_RESULT_BASE, null);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private static ZkService buildZKService(String zkconnect) {
        try {
            return new ZkService(zkconnect);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void senderPrepare(boolean cleanOldNodes) {
        try {
            String parentNodePath = buildRoot();
            if (!zk.isExists(parentNodePath)) {
                zk.createNode(parentNodePath, null);
            }
            if (cleanOldNodes) {
                List<String> children = zk.getChildren(parentNodePath);
                for (String child : children) {
                    zk.deleteNode(buildNode(child));
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void send(String node, CtlMessageResult result, boolean prepare, boolean cleanOldNodes) {
        try {
            if (prepare) {
                senderPrepare(cleanOldNodes);
                zk.setData(buildRoot(), JSON.toJSONString(result.getOriginalMessage(), true).getBytes(UTF8));
            }
            String nodePath = buildNode(node);
            if (zk.isExists(nodePath)) {
                zk.setData(nodePath, result.toString().getBytes(UTF8));
            } else {
                zk.createNode(nodePath, result.toString().getBytes(UTF8));
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            try {
                zk.close();
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    private final String buildRoot() {
        return CTL_MST_RESULT_BASE + (parentNode.startsWith("/") ? parentNode : "/" + parentNode);
    }

    private final String buildNode(String node) {
        return buildRoot() + "/" + node;
    }
}
