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


package com.creditease.dbus.service;

import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.base.com.creditease.dbus.utils.RequestSender;
import com.creditease.dbus.constant.ServiceNames;
import com.creditease.dbus.domain.model.DataTable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.MessageFormat;
import java.util.Set;

@Service
public class DataHubService {

    private static Logger logger = LoggerFactory.getLogger(DataHubService.class);

    @Autowired
    private RequestSender sender;
    @Autowired
    private ToolSetService toolSetService;

    public void startTable(Long schemaId, String tableName) throws Exception {
        String query = "/tables/findBySchemaIdAndTableName?schemaId={schemaId}&tableName={tableName}";
        DataTable dataTable = sender.get(ServiceNames.KEEPER_SERVICE, query, schemaId, tableName).getBody().getPayload(DataTable.class);
        dataTable.setStatus("ok");
        sender.post(ServiceNames.KEEPER_SERVICE, "/tables/update", dataTable).getBody();
        toolSetService.reloadConfig(dataTable.getDsId(), dataTable.getDsName(), ToolSetService.APPENDER_RELOAD_CONFIG);
    }

    public void createTopic(String topic, String user) throws Exception {
        Set<String> topics = toolSetService.getTopics(null, null);
        //topic不存在,需要创建
        if (!topics.contains(topic)) {
            //调用addTopicAcl.sh脚本,第一个参数是user,第二个参数是topic
            String currentPath = System.getProperty("user.dir");
            String cmd = MessageFormat.format("sh {2}/addTopicAcl.sh {0} {1}", user, topic, currentPath);

            logger.info("add cal command: {}", cmd);
            Process process = Runtime.getRuntime().exec(cmd);
            int exitValue = process.waitFor();
            if (0 != exitValue) {
                logger.error("[add table]call shell failed. cmd:{}, error code is{} :", cmd, exitValue);
                throw new RuntimeException("add acl error");
            }
        }
    }

    public ResultEntity getAllDataSourceInfo(String dsName) {
        String query = "/datahub/getAllDataSourceInfo";
        if (StringUtils.isNotBlank(dsName)) {
            query = query + "?dsName=" + dsName;
        }
        return sender.get(ServiceNames.KEEPER_SERVICE, query).getBody();
    }
}
