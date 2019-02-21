/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2018 Bridata
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

package com.creditease.dbus.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.parser.Feature;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.base.com.creditease.dbus.utils.RequestSender;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.constant.ServiceNames;
import com.creditease.dbus.domain.model.FullPullHistory;
import com.creditease.dbus.utils.JsonFormatUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;


/**
 * Created by xiancangao on 2018/04/16.
 */
@Service
public class FullPullHistoryService {
    @Autowired
    private RequestSender sender;
    @Autowired
    private IZkService zkService;

    private static final String MS_SERVICE = ServiceNames.KEEPER_SERVICE;

    public ResultEntity search(String queryString) {
        ResponseEntity<ResultEntity> result = sender.get(MS_SERVICE, "/fullPullHistory/search", queryString);
        return result.getBody();
    }

    public ResultEntity getDSNames(String queryString) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/fullPullHistory/datasourceNames", queryString);
        return result.getBody();
    }

    public ResultEntity queryProjectNames(String queryString) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/fullPullHistory/project-names", queryString);
        return result.getBody();
    }

    public ResultEntity update(FullPullHistory fullPullHistory) {
        return sender.post(ServiceNames.KEEPER_SERVICE, "/fullPullHistory/update", fullPullHistory).getBody();
    }

    public void clearFullPullAlarm(String dsName) throws Exception {
        List<String> paths = new ArrayList<>();
        //源端
        String monitorRootPath = Constants.FULL_PULL_MONITOR_ROOT + "/" + dsName;
        getMonitorPaths(monitorRootPath, paths);
        //多租户
        monitorRootPath = Constants.FULL_PULL_PROJECTS_MONITOR_ROOT;
        for (String projectName : zkService.getChildren(monitorRootPath)) {
            getMonitorPaths(monitorRootPath + "/" + projectName + "/" + dsName, paths);
        }

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String currentTimeStampString = formatter.format(new Date());
        boolean updateFlag = false;
        for (String path : paths) {
            byte[] data = zkService.getData(path);
            LinkedHashMap<String, Object> monitorData = JSON.parseObject(new String(data, KeeperConstants.UTF8),
                    new TypeReference<LinkedHashMap<String, Object>>() {
                    }, Feature.OrderedField);
            Object status = monitorData.get("Status");
            Object endTime = monitorData.get("EndTime");

            if (status != null && StringUtils.isNotBlank(status.toString()) && (status.equals("ending") || status.equals("abort"))) {
                if (endTime != null && StringUtils.isNotBlank(endTime.toString())) {
                    try {
                        formatter.parse(endTime.toString());
                        updateFlag = false;
                    } catch (Exception e) {
                        updateFlag = true;
                    }
                }else{
                    updateFlag = true;
                }
            }else{
                updateFlag = true;
            }
            if (updateFlag) {
                monitorData.put("UpdateTime", currentTimeStampString);
                monitorData.put("EndTime", currentTimeStampString);
                monitorData.put("Status", "abort");
                String format = JsonFormatUtils.toPrettyFormat(JSON.toJSONString(monitorData, SerializerFeature.WriteMapNullValue));
                zkService.setData(path, format.getBytes(KeeperConstants.UTF8));
            }
        }
    }

    private void getMonitorPaths(String root, List<String> paths) throws Exception {
        List<String> children = null;
        try {
            if (!zkService.isExists(root)) return;
            children = zkService.getChildren(root);
            if (children != null && children.size() > 0) {
                for (String nodeName : children) {
                    getMonitorPaths(root + "/" + nodeName, paths);
                }
            }
            //^\d{4}-\d{1,2}-\d{1,2}\s+\d{2}\.\d{2}\.\d{2}\.\d{3}\s+-\s+\d+$
            //判断是监控节点,暂时这么写,例如 2018-10-29 15.00.02.269 - 0
            if (root.contains(" - ")) {
                paths.add(root);
            }
        } catch (Exception e) {
            throw e;
        }
    }

}
