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

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.base.com.creditease.dbus.utils.RequestSender;
import com.creditease.dbus.constant.ServiceNames;
import com.creditease.dbus.domain.model.DataTable;
import com.creditease.dbus.domain.model.FullPullHistory;
import com.creditease.dbus.domain.model.ProjectTopoTable;
import com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * This is Description
 *
 * @author xiancangao
 * @date 2019/02/19
 */
@Service
public class BatchFullPullService {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private RequestSender sender;
    @Autowired
    private FullPullService fullPullService;

    @Async
    public void batchGlobalfullPull(Map<String, Object> map) throws Exception {
        logger.info("批量拉全量开始..........");
        logger.info("批量拉全量请求参数:{}", map);

        String outputTopic = (String) map.get("outputTopic");
        String hdfsRootPath = (String) map.get("hdfsRootPath");
        Boolean isProject = (Boolean) map.get("isProject");
        ArrayList<Integer> ids = (ArrayList<Integer>) map.get("ids");
        if (isProject) {
            //处理多租户
            ResponseEntity<ResultEntity> result = sender.post(ServiceNames.KEEPER_SERVICE, "/projectTable/search-tables", ids);
            List<Map<String, Object>> topoTableList = result.getBody().getPayload(new TypeReference<List<Map<String, Object>>>() {
            });
            ConcurrentMap<String, List<Map<String, Object>>> schemaTopoTables = topoTableList.stream()
                    .collect(Collectors.groupingByConcurrent(t -> String.format("%s&%s", t.get("dsName"), t.get("schemaName"))));

            for (List<Map<String, Object>> maps : schemaTopoTables.values()) {
                executeProjectfullPull(outputTopic, maps);
            }
        } else {
            //处理源端
            ResponseEntity<ResultEntity> result = sender.post(ServiceNames.KEEPER_SERVICE, "/tables/searchTableByIds", ids);
            List<DataTable> tableList = result.getBody().getPayload(new TypeReference<List<DataTable>>() {
            });
            ConcurrentMap<String, List<DataTable>> schemaTables = tableList.stream()
                    .collect(Collectors.groupingByConcurrent(t -> String.format("%s&%s", t.getDsName(), t.getSchemaName())));
            for (List<DataTable> tables : schemaTables.values()) {
                executeSourcefullPull(outputTopic, hdfsRootPath, tables);
            }
        }
        logger.info("批量拉全量结束..........");
    }

    /**
     * 多租户批量拉全量
     */
    public void executeProjectfullPull(String outputTopic, List<Map<String, Object>> topoTables) throws Exception {
        Map<String, Object> payloadMap = null;
        for (Map<String, Object> map : topoTables) {
            ProjectTopoTable topoTable = new ProjectTopoTable();
            topoTable.setId((Integer) map.get("topoTableId"));
            topoTable.setProjectId((Integer) map.get("projectId"));
            topoTable.setSinkId((Integer) map.get("sinkId"));

            DataTable dataTable = new DataTable();
            dataTable.setDsId((Integer) map.get("dsId"));
            dataTable.setDsName((String) map.get("dsName"));
            dataTable.setSchemaName((String) map.get("schemaName"));
            dataTable.setTableName((String) map.get("tableName"));
            dataTable.setPhysicalTableRegex((String) map.get("physicalTableRegex"));
            dataTable.setCtrlTopic((String) map.get("ctrlTopic"));
            dataTable.setDsType((String) map.get("dsType"));
            dataTable.setMasterUrl((String) map.get("masterUrl"));
            dataTable.setDbusUser((String) map.get("dbusUser"));
            dataTable.setDbusPassword((String) map.get("dbusPwd"));
            dataTable.setOutputTopic((String) map.get("outputTopic"));

            JSONObject message = fullPullService.buildProjectFullPullMessage(topoTable, dataTable, outputTopic);

            // 获取opts需要读取result topic,这个操作比较耗时,同一个schema只需要获取一次即可
            JSONObject payload = message.getJSONObject("payload");
            if (payloadMap == null) {
                String resultTopic = (String) map.get("outputTopic");
                payloadMap = fullPullService.setOPTS(resultTopic, payload);
            }
            payload.put("POS", payloadMap.get("POS"));
            payload.put("OP_TS", payloadMap.get("OP_TS"));

            //生成fullPullHistory对象
            FullPullHistory fullPullHistory = new FullPullHistory();
            fullPullHistory.setId(message.getLong("id"));
            fullPullHistory.setType("indepent");
            fullPullHistory.setDsName(dataTable.getDsName());
            fullPullHistory.setSchemaName(dataTable.getSchemaName());
            fullPullHistory.setTableName(dataTable.getTableName());
            fullPullHistory.setState("init");
            fullPullHistory.setInitTime(new Date(fullPullHistory.getId()));
            fullPullHistory.setUpdateTime(fullPullHistory.getInitTime());
            fullPullHistory.setTargetSinkTopic(outputTopic);
            JSONObject projectJson = message.getJSONObject("project");
            fullPullHistory.setProjectName(projectJson.getString("name"));
            fullPullHistory.setTopologyTableId(topoTable.getId());
            fullPullHistory.setTargetSinkId(topoTable.getSinkId());

            //发送消息
            fullPullService.sendMessage(dataTable, message.toJSONString(), fullPullHistory);
        }
    }

    public void executeSourcefullPull(String outputTopic, String hdfsRootPath, List<DataTable> tables) throws Exception {
        Map<String, Object> payloadMap = null;
        for (DataTable table : tables) {
            JSONObject message = fullPullService.buildSourceFullPullMessage(table, outputTopic, hdfsRootPath);
            JSONObject payload = message.getJSONObject("payload");
            if (payloadMap == null) {
                String resultTopic = tables.get(0).getOutputTopic();
                payloadMap = fullPullService.setOPTS(resultTopic, payload);
            }
            payload.put("POS", payloadMap.get("POS"));
            payload.put("OP_TS", payloadMap.get("OP_TS"));

            //生成fullPullHistory对象
            FullPullHistory fullPullHistory = new FullPullHistory();
            fullPullHistory.setId(message.getLong("id"));
            fullPullHistory.setType("indepent");
            fullPullHistory.setDsName(table.getDsName());
            fullPullHistory.setSchemaName(table.getSchemaName());
            fullPullHistory.setTableName(table.getTableName());
            fullPullHistory.setState("init");
            fullPullHistory.setInitTime(new Date(fullPullHistory.getId()));
            fullPullHistory.setUpdateTime(fullPullHistory.getInitTime());
            fullPullHistory.setTargetSinkTopic(outputTopic);
            //发送消息
            fullPullService.sendMessage(table, message.toJSONString(), fullPullHistory);
        }
    }
}
