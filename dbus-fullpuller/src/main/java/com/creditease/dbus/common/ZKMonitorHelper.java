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


package com.creditease.dbus.common;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.ZkService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * This is Description
 *
 * @author xiancangao
 * @date 2018/12/30
 */
public class ZKMonitorHelper {

    private static Logger logger = LoggerFactory.getLogger(ZKMonitorHelper.class);

    public static String getCurrentPendingZKNodePath(String monitorRoot, String dsName, JSONObject reqJson) {
        JSONObject projectJson = reqJson.getJSONObject(FullPullConstants.REQ_PROJECT);
        String pendingZKNodePath;
        if (projectJson != null && !projectJson.isEmpty()) {
            String projectName = projectJson.getString(FullPullConstants.REQ_PROJECT_NAME);
            String projectId = projectJson.getString(FullPullConstants.REQ_PROJECT_ID);
            pendingZKNodePath = FullPullConstants.FULL_PULL_PROJECTS_MONITOR_ROOT + "/" + projectName + "_" + projectId + "/" + dsName;
        } else {
            pendingZKNodePath = monitorRoot + "/" + dsName;
        }
        return pendingZKNodePath;
    }

    public static List<String> getPendingZKNodePath(String monitorRoot, String dsName, ZkService zkService) throws Exception {
        List<String> pendingZKNodePaths = new ArrayList<>();
        try {
            //源端全量pending节点
            String pendingZKNodePath = monitorRoot + "/" + dsName;
            if (zkService.isExists(pendingZKNodePath)) {
                pendingZKNodePaths.add(pendingZKNodePath);
            }
            //多租户全量pending节点
            String projectsZKNodePath = monitorRoot + "/Projects";
            for (String projectZKNodePath : zkService.getChildren(projectsZKNodePath)) {
                pendingZKNodePath = projectsZKNodePath + "/" + projectZKNodePath + "/" + dsName;
                if (zkService.isExists(pendingZKNodePath)) {
                    pendingZKNodePaths.add(pendingZKNodePath);
                }
            }
            return pendingZKNodePaths;
        } catch (Exception e) {
            logger.error("Exception happend when get pending task zk node.");
            throw e;
        }
    }

    public static String getMonitorNodePath(String monitorRoot, String dbNameSpace, JSONObject reqJson) throws Exception {
        String monitorNodePath = null;
        JSONObject projectJson = reqJson.getJSONObject(FullPullConstants.REQ_PROJECT);
        if (projectJson != null && !projectJson.isEmpty()) {
            int projectId = projectJson.getIntValue(FullPullConstants.REQ_PROJECT_ID);
            String projectName = projectJson.getString(FullPullConstants.REQ_PROJECT_NAME);
            monitorNodePath = String.format("%s/%s/%s_%s/%s", monitorRoot, "Projects", projectName, projectId, dbNameSpace);
        } else {
            monitorNodePath = buildZkPath(monitorRoot, dbNameSpace);
        }
        return monitorNodePath;
    }

    private static String buildZkPath(String parent, String child) {
        return parent + "/" + child;
    }

}
