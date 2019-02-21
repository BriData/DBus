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
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.base.com.creditease.dbus.utils.RequestSender;
import com.creditease.dbus.bean.ExecuteSqlBean;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.ControlMessage;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.commons.log.processor.adapter.LogFilebeatAdapter;
import com.creditease.dbus.commons.log.processor.adapter.LogFlumeAdapter;
import com.creditease.dbus.commons.log.processor.adapter.LogUmsAdapter;
import com.creditease.dbus.commons.log.processor.parse.RuleGrammar;
import com.creditease.dbus.commons.log.processor.rule.impl.Rules;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.constant.ServiceNames;
import com.creditease.dbus.domain.model.*;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.utils.ControlMessageSenderProvider;
import com.creditease.dbus.utils.ControlMessageSender;
import com.creditease.dbus.utils.OrderedProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Joiner;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.SystemUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.creditease.dbus.constant.KeeperConstants.GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS;


/**
 * Created by xiancangao on 2018/05/16.
 */
@Service
public class TableService {
    @Autowired
    private RequestSender sender;
    @Autowired
    private IZkService zkService;
    @Autowired
    private ToolSetService toolSetService;
    @Autowired
    private AutoDeployDataLineService autoDeployDataLineService;

    private static Logger logger = LoggerFactory.getLogger(TableService.class);
    private static final String INITIAL_LOAD_DATA = "load-data";


    public Integer activateDataTable(Integer id, Map<String, String> map) throws Exception {
        ActiveTableParam param;
        try {
            param = ActiveTableParam.build(map);
        } catch (Exception e) {
            logger.error("activateDataTable : param error,tableId :{}, param:{}", id, map);
            return MessageCode.TABLE_PARAM_FORMAT_ERROR;
        }
        logger.info("Receive activateDataTable request, parameter[id:{}, param:{}]", id, JSON.toJSONString(param));
        DataTable table = this.getTableById(id);
        if (table == null) {
            logger.info("tables : can not found table by id.");
            return MessageCode.TABLE_NOT_FOUND_BY_ID;
        }
        if (INITIAL_LOAD_DATA.equalsIgnoreCase(map.get("type"))) {
            DbusDatasourceType dsType = DbusDatasourceType.parse(table.getDsType());
            InitialLoadService ilService = InitialLoadService.getService();

            FullPullHistory fullPullHistory = new FullPullHistory();
            Date date = new Date();
            fullPullHistory.setId(date.getTime());
            fullPullHistory.setType("normal");
            fullPullHistory.setDsName(table.getDsName());
            fullPullHistory.setSchemaName(table.getSchemaName());
            fullPullHistory.setTableName(table.getTableName());
            fullPullHistory.setState("init");
            fullPullHistory.setInitTime(date);
            fullPullHistory.setUpdateTime(date);
            sender.post(ServiceNames.KEEPER_SERVICE, "/fullPullHistory/create", fullPullHistory);

            if (DbusDatasourceType.ORACLE == dsType) {
                logger.info("Activate oracle table");
                // 处理oracle拉全量
                ilService.oracleInitialLoadBySql(table, date.getTime());
            } else if (DbusDatasourceType.MYSQL == dsType) {
                logger.info("Activate mysql table");
                // 处理mysql拉全量
                ilService.mysqlInitialLoadBySql(table, date.getTime());
            } else if (DbusDatasourceType.MONGO == dsType) {
                logger.info("Activate mysql table");
                // 处理mongo拉全量
                ilService.mongoInitialLoadBySql(table, date.getTime());
            }
            else {
                logger.error("Illegal datasource type:" + table.getDsType());
                return MessageCode.TYPE_OF_TABLE_CAN_NOT_FULLPULL;
                // throw new IllegalArgumentException("Illegal datasource type:" + table.getDsType());
            }
        } else {

            // 不用拉全量的情况下直接发送 APPENDER_TOPIC_RESUME message 激活 appender
            ControlMessageSender sender = ControlMessageSenderProvider.getControlMessageSender(zkService);
            ControlMessage message = new ControlMessage();
            message.setId(System.currentTimeMillis());
            message.setFrom(KeeperConstants.CONTROL_MESSAGE_SENDER_NAME);
            message.setType("APPENDER_TOPIC_RESUME");

            DataSchema schema = this.getDataSchemaById(table.getSchemaId());
            message.addPayload("topic", schema.getSrcTopic());
            message.addPayload("SCHEMA_NAME", table.getSchemaName());
            message.addPayload("TABLE_NAME", table.getTableName());
            message.addPayload("STATUS", KeeperConstants.OK);
            message.addPayload("VERSION", param.getVersion());

            try {
                sender.send(table.getCtrlTopic(), message);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return MessageCode.EXCEPTION_ON_SEND_MESSAGE;
            }
            logger.info("Control message sent, message:{}", message.toJSONString());
            // 如果需要更新version则
            if (param.getVersion() > 0) {
                TableVersion tableVersion = new TableVersion();
                tableVersion.setVersion(param.getVersion());
                tableVersion.setId(table.getVerId());
                this.updateVersion(tableVersion);
            }
        }

        logger.info("Activate DataTable request process ok");
        return null;
    }

    public Integer deactivateDataTable(Integer id) throws Exception {
        logger.info("Receive deactivateDataTable request, parameter[id:{}, param:{}]", id);
        DataTable table = this.getTableById(id);
        if (table == null) {
            logger.info("tables : can not found table by id.");
            return MessageCode.TABLE_NOT_FOUND_BY_ID;
        }

        Integer result = validateTableStatus(table);
        if (result != null) {
            return result;
        }
        // 不用拉全量的情况下直接发送 APPENDER_TOPIC_RESUME message 激活 appender
        ControlMessageSender sender = ControlMessageSenderProvider.getControlMessageSender(zkService);
        ControlMessage message = new ControlMessage();
        message.setId(System.currentTimeMillis());
        message.setFrom(KeeperConstants.CONTROL_MESSAGE_SENDER_NAME);
        message.setType("APPENDER_TOPIC_RESUME");

        DataSchema schema = this.getDataSchemaById(table.getSchemaId());
        message.addPayload("topic", schema.getSrcTopic());
        message.addPayload("SCHEMA_NAME", table.getSchemaName());
        message.addPayload("TABLE_NAME", table.getTableName());
        message.addPayload("STATUS", KeeperConstants.ABORT);
        message.addPayload("VERSION", 0);

        sender.send(table.getCtrlTopic(), message);
        logger.info("Control message sent, message:{}", message.toJSONString());
        return null;
    }

    /**
     * 根据tableId查询脱敏列
     *
     * @param tableId
     * @return
     */
    public ResultEntity desensitization(Integer tableId) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/tables/getDesensitizationInfo/{id}", tableId);
        return result.getBody();

    }

    public ResultEntity fetchTableColumns(Integer tableId) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/tables/fetchTableColumns/{0}", tableId);
        return result.getBody();
    }

    public ResultEntity updateTable(DataTable dataTable) {
        ResponseEntity<ResultEntity> result = sender.post(ServiceNames.KEEPER_SERVICE, "/tables/update", dataTable);

        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
            return result.getBody();

        //TODO mongo的表更新完后，自动reload
        DataTable table = getTableById(dataTable.getId());//Integer dsId, String dsName, String dsType) {
        if (DbusDatasourceType.MONGO == DbusDatasourceType.parse(table.getDsType())
                && KeeperConstants.OK.equals(table.getStatus())) {
            toolSetService.reloadMongoCatch(table.getDsId(), table.getDsName(), table.getDsType());
        }
        return result.getBody();


    }

    public ResultEntity deleteTable(int tableId) throws Exception {
        DataTable table = this.getTableById(tableId);
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/tables/delete/{0}", tableId);
        if (result.getBody().getStatus() != 0) {
            return result.getBody();
        }
        ResultEntity resultEntity = new ResultEntity();
        if (table.getDsType().equalsIgnoreCase("oracle")) {
            String dsName = table.getDsName();
            if (autoDeployDataLineService.isAutoDeployOgg(dsName)) {
                String schemaName = table.getSchemaName();
                HashMap<String, String> map = new HashMap<>();
                map.put("dsName", dsName);
                map.put("schemaName", schemaName);
                map.put("tableNames", table.getTableName());
                resultEntity.setStatus(autoDeployDataLineService.deleteOracleTable(map));
            }
        }
        return resultEntity;
    }

    public ResultEntity confirmStatusChange(int tableId) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/tables/confirmStatusChange/{0}", tableId);
        return result.getBody();
    }

    public ResultEntity getVersionListByTableId(int tableId) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/table-version/get-by-table-id/{0}", tableId);
        return result.getBody();
    }

    public ResultEntity getVersionDetail(int versionId1, int versionId2) {
        return sender.get(ServiceNames.KEEPER_SERVICE, "/tables/get-version-detail/{0}/{1}",
                versionId1, versionId2).getBody();
    }

    public ResultEntity changeDesensitization(Map<String, Map<String, Object>> param) {
        return sender.post(ServiceNames.KEEPER_SERVICE, "/tables/change-desensitization",
                param).getBody();
    }

    public ResultEntity getAllRuleGroup(Integer tableId) {
        return sender.get(ServiceNames.KEEPER_SERVICE, "/tables/get-all-rule-group/{0}", tableId).getBody();
    }

    /**
     * 根据ID获取指定table
     *
     * @param id 数据源ID
     * @return 指定的数据源
     */
    public DataTable getTableById(Integer id) {
        //TODO
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/tables/get/{0}", id);
        return result.getBody().getPayload(DataTable.class);
    }

    /**
     * 根据schema的ID查询相关table
     *
     * @param schemaId 数据源ID,不存在则查询所有table
     * @return 返回满足条件的table列表
     */
    private List<DataTable> getTablesBySchemaID(Integer schemaId) {
        //TODO
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/tables/get-by-schema-id/{0}", schemaId);
        return result.getBody().getPayload(new TypeReference<List<DataTable>>() {
        });
    }

    /**
     * 根据tableId的schemaId查询DataSchema
     *
     * @param schemaId 数据源ID
     * @return 返回满足条件的EncodeColumn列表
     */
    public DataSchema getDataSchemaById(Integer schemaId) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/dataschema/get/{0}", schemaId);
        return result.getBody().getPayload(DataSchema.class);
    }

    /**
     * 根据tableId的dsId查询DataSchema
     *
     * @param dsId 数据源ID
     * @return 返回满足条件的EncodeColumn列表
     */
    private DataSource getDataSourceById(Integer dsId) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/datasource/{0}", dsId);
        return result.getBody().getPayload(DataSource.class);
    }

    /**
     * 更新tableVersion
     *
     * @param tableVersion
     * @return
     */
    private DataSource updateVersion(TableVersion tableVersion) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/table-version/update", tableVersion);
        return result.getBody().getPayload(DataSource.class);
    }

    /**
     * 根据Id获取gTableVersionById
     *
     * @param id
     * @return
     */
    private TableVersion getTableVersionById(Integer id) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/table-version/get/{0}", id);
        return result.getBody().getPayload(TableVersion.class);
    }

    private Integer validateTableStatus(DataTable table) throws Exception {
        DataTable waitingTable = null;
        List<DataTable> list = this.getTablesBySchemaID(table.getSchemaId());
        for (DataTable dataTable : list) {
            if (KeeperConstants.WAITING.equals(dataTable.getStatus())) {
                waitingTable = dataTable;
                break;
            }
        }
        // 如果没有处于waiting状态的表,则返回成功
        if (waitingTable == null) {
            return null;
        }
        Integer result = validateZookeeperNode(waitingTable);
        if (result == MessageCode.EXCEPTION) {
            // 没有找到节点的情况
            // 判断zk中节点的状态时间是否超过15分钟,超过则
            boolean expire = System.currentTimeMillis() - waitingTable.getCreateTime().getTime() > 0.5 * 60 * 1000;
            if (expire) {
                return null;
            } else {
                return MessageCode.TABLE_IS_WAITING_FOR_INITIAL_LOAD;
            }
        }
        return null;
    }

    private Integer validateZookeeperNode(DataTable table) throws Exception {
        TableVersion version = this.getTableVersionById(table.getVerId());
        if (version == null) {
            logger.info("tables : can not found tableVersion by id.");
            return MessageCode.TABLE_VERSION_NOT_FOUND_BY_ID;
        }

        byte[] data;
        String zkNode = Joiner.on("/").join("/DBus/FullPuller", table.getDsName(), table.getSchemaName(),
                table.getTableName(), version.getVersion());

        try {
            data = zkService.getData(zkNode);
        } catch (Exception e) {
            // 没有节点的情况下,要继续判断table保持waiting状态的时间
            String info = String.format("zookeeper node [%s] not exists", zkNode);
            logger.warn(info);
            return MessageCode.EXCEPTION;
        }
        if (data != null && data.length > 0) {
            String json = new String(data, KeeperConstants.UTF8);
            InitialLoadStatus status = JSON.parseObject(json, InitialLoadStatus.class);
            if (Constants.FULL_PULL_STATUS_SPLITTING.equals(status.getStatus())) {
                String info = String.format("Table %s is waiting for loading data.", table.getTableName());
                logger.info(info);
                return MessageCode.TABLE_IS_WAITING_FOR_LOADING_DATA;
            } else if (Constants.FULL_PULL_STATUS_PULLING.equals(status.getStatus())) {
                boolean expire = System.currentTimeMillis() - status.getUpdateTime().getTime() > 15 * 60 * 1000;
                if (expire) {
                    String info = String.format("Table[%s] data loading is running, but expired, last update time is %s",
                            table.getTableName(), status.getUpdateTime());
                    logger.info(info);
                    return MessageCode.TABLE_DATA_LOADING_IS_EXPIRED;
                }
                return MessageCode.INITIAL_LOAD_IS_RUNNING;
            } else if (Constants.FULL_PULL_STATUS_ENDING.equals(status.getStatus())) {
                return null;
            } else {
                throw new IllegalStateException("Illegal state[" + status.getStatus() + "] of node " + zkNode);
            }
        }
        // 要继续判断table保持waiting状态的时间
        return null;
    }

    public ResultEntity updateRuleGroup(String param) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/tables/update-rule-group", param);
        return result.getBody();
    }

    public ResultEntity deleteRuleGroup(Integer groupId) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/tables/delete-rule-group/{0}", groupId);
        return result.getBody();
    }

    public ResultEntity addGroup(String param) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/tables/add-group", param);
        return result.getBody();
    }

    public ResultEntity cloneRuleGroup(String param) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/tables/clone-rule-group", param);
        return result.getBody();
    }

    public ResultEntity diffGroupRule(Integer tableId) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/tables/diff-group-rule/{0}", tableId);
        return result.getBody();
    }

    public ResultEntity upgradeVersion(String param) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/tables/up-grade-version", param);
        return result.getBody();
    }

    public Map<String, Object> getAllRules(Map<String, Object> map) throws Exception {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/tables/get-all-rules/{0}",
                Integer.parseInt(map.get("groupId").toString()));
        List<DataTableRule> rules = result.getBody().getPayload(new TypeReference<List<DataTableRule>>() {
        });

        // 获取该表的数据源topic
        DataSource dataSource = this.getDataSourceById(Integer.parseInt(map.get("dsId").toString()));
        String topic = dataSource.getTopic();

        // 获取该表的数据源offset
        Properties consumerProps = zkService.getProperties(KeeperConstants.KEEPER_CONSUMER_CONF);
        consumerProps.setProperty("client.id", "plain.log.reader");
        consumerProps.setProperty("group.id", "plain.log.reader");
        Properties globalConf = zkService.getProperties(KeeperConstants.GLOBAL_CONF);
        consumerProps.setProperty(GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS, globalConf.getProperty(GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS));
        KafkaConsumer<String, String> consumer = null;
        try {
            consumer = new KafkaConsumer(consumerProps);
            TopicPartition dataTopicPartition = new TopicPartition(topic, 0);
            List<TopicPartition> topics = Arrays.asList(dataTopicPartition);
            consumer.assign(topics);
            consumer.seekToEnd(topics);
            long offset = consumer.position(dataTopicPartition);

            Map<String, Object> res = new HashMap<>();
            res.put("topic", topic);
            res.put("offset", offset);
            res.put("rules", rules);
            return res;
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }

    public ResultEntity saveAllRules(Map<String, Object> map) {
        ResponseEntity<ResultEntity> result = sender.post(ServiceNames.KEEPER_SERVICE, "/tables/save-all-rules", map);
        return result.getBody();
    }

    public ResultEntity executeRules(Map<String, Object> map) throws Exception {
        ResultEntity resultEntity = new ResultEntity();
        Map<String, Object> kafkaData = getKafkaPlainLogContent(map);
        List<List<String>> keysList = (List<List<String>>) kafkaData.get("keysList");
        List<List<String>> valuesList = (List<List<String>>) kafkaData.get("valuesList");
        List<RuleInfo> executeRules = JSON.parseArray(map.get("executeRules").toString(), RuleInfo.class);
        List<List<String>> data = new ArrayList<>();

        if (valuesList == null || executeRules == null) {
            resultEntity.setPayload(valuesList);
            return resultEntity;
        }

        DbusDatasourceType dsType = DbusDatasourceType.parse(map.get("dsType").toString().toLowerCase());

        List<Long> offset = new ArrayList<>();
        long kafkaOffset = Long.parseLong(map.get("kafkaOffset").toString());

        for (int i = 0; i < valuesList.size(); i++) {

            List<String> rowList = valuesList.get(i);

            if (dsType.equals(DbusDatasourceType.LOG_UMS)) {
                if (executeRules.size() == 0) {
                    data.add(rowList);
                    offset.add(kafkaOffset + i);
                } else {
                    LogUmsAdapter adapter = new LogUmsAdapter(rowList.get(0));
                    while (adapter.hasNext()) {
                        rowList = new ArrayList() {{
                            add(adapter.next());
                        }};
                        for (RuleInfo rule : executeRules) {
                            String ruleGramar = rule.getRuleGrammar();
                            List<RuleGrammar> grammar = JSON.parseArray(ruleGramar, RuleGrammar.class);
                            Rules rules = Rules.fromStr(rule.getRuleTypeName());
                            rowList = rules.getRule().transform(rowList, grammar, rules);
                            if (rowList.isEmpty()) break;
                        }
                        if (!rowList.isEmpty()) {
                            data.add(rowList);
                            offset.add(kafkaOffset + i);
                        }
                    }
                }
            } else if (dsType.equals(DbusDatasourceType.LOG_LOGSTASH)
                    || dsType.equals(DbusDatasourceType.LOG_LOGSTASH_JSON)) {
                for (RuleInfo rule : executeRules) {
                    String ruleGramar = rule.getRuleGrammar();
                    List<RuleGrammar> grammar = JSON.parseArray(ruleGramar, RuleGrammar.class);
                    Rules rules = Rules.fromStr(rule.getRuleTypeName());
                    rowList = rules.getRule().transform(rowList, grammar, rules);
                    if (rowList.isEmpty()) break;
                }
                if (!rowList.isEmpty()) {
                    data.add(rowList);
                    offset.add(kafkaOffset + i);
                }
            } else if (dsType.equals(DbusDatasourceType.LOG_FLUME)) {
                LogFlumeAdapter adapter = new LogFlumeAdapter(keysList.get(i).get(0), rowList.get(0));
                while (adapter.hasNext()) {
                    rowList = new ArrayList() {{
                        add(adapter.next());
                    }};
                    for (RuleInfo rule : executeRules) {
                        String ruleGramar = rule.getRuleGrammar();
                        List<RuleGrammar> grammar = JSON.parseArray(ruleGramar, RuleGrammar.class);
                        Rules rules = Rules.fromStr(rule.getRuleTypeName());
                        rowList = rules.getRule().transform(rowList, grammar, rules);
                        if (rowList.isEmpty()) break;
                    }
                    if (!rowList.isEmpty()) {
                        data.add(rowList);
                        offset.add(kafkaOffset + i);
                    }
                }
            } else if (dsType.equals(DbusDatasourceType.LOG_FILEBEAT)) {
                LogFilebeatAdapter adapter = new LogFilebeatAdapter(rowList.get(0));
                while (adapter.hasNext()) {
                    rowList = new ArrayList() {{
                        add(adapter.next());
                    }};
                    for (RuleInfo rule : executeRules) {
                        String ruleGramar = rule.getRuleGrammar();
                        List<RuleGrammar> grammar = JSON.parseArray(ruleGramar, RuleGrammar.class);
                        Rules rules = Rules.fromStr(rule.getRuleTypeName());
                        rowList = rules.getRule().transform(rowList, grammar, rules);
                        if (rowList.isEmpty()) break;
                    }
                    if (!rowList.isEmpty()) {
                        data.add(rowList);
                        offset.add(kafkaOffset + i);
                    }
                }
            }
        }

        HashMap<String, Object> ret = new HashMap<>();
        ret.put("data", data);
        ret.put("offset", offset);
        if (executeRules.size() == 0) {
            if (dsType.equals(DbusDatasourceType.LOG_UMS)) {
                ret.put("dataType", "UMS");
            } else if (dsType.equals(DbusDatasourceType.LOG_LOGSTASH)
                    || dsType.equals(DbusDatasourceType.LOG_LOGSTASH_JSON)
                    || dsType.equals(DbusDatasourceType.LOG_FLUME)
                    || dsType.equals(DbusDatasourceType.LOG_FILEBEAT)) {
                ret.put("dataType", "JSON");
            }
        } else {
            String lastRule = executeRules.get(executeRules.size() - 1).getRuleTypeName();
            if (lastRule.equals(Rules.FLATTENUMS.name)
                    || lastRule.equals(Rules.KEYFILTER.name)) {
                ret.put("dataType", "JSON");
            } else if (lastRule.equals(Rules.SAVEAS.name)) {
                ret.put("dataType", "FIELD");
            } else {
                ret.put("dataType", "STRING");
            }
        }

        resultEntity.setPayload(ret);
        return resultEntity;
    }

    private Map<String, Object> getKafkaPlainLogContent(Map<String, Object> map) {
        String topic = map.get("kafkaTopic").toString();
        long offset = Long.parseLong(map.get("kafkaOffset").toString());
        long count = Long.parseLong(map.get("kafkaCount").toString());
        DbusDatasourceType dsType = DbusDatasourceType.parse(map.get("dsType").toString().toLowerCase());
        KafkaConsumer<String, String> consumer = null;
        try {
            Properties consumerProps = zkService.getProperties(KeeperConstants.KEEPER_CONSUMER_CONF);
            consumerProps.setProperty("client.id", "plain.log.reader");
            consumerProps.setProperty("group.id", "plain.log.reader");
            Properties globalConf = zkService.getProperties(KeeperConstants.GLOBAL_CONF);
            consumerProps.setProperty(GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS, globalConf.getProperty(GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS));
            consumer = new KafkaConsumer(consumerProps);

            TopicPartition dataTopicPartition = new TopicPartition(topic, 0);
            List<TopicPartition> topics = Arrays.asList(dataTopicPartition);
            consumer.assign(topics);
            consumer.seek(dataTopicPartition, offset);

            List<List<String>> valuesList = new ArrayList<>();
            List<List<String>> keysList = new ArrayList<>();
            while (valuesList.size() < count) {
                ConsumerRecords<String, String> records = consumer.poll(3000);
                for (ConsumerRecord<String, String> record : records) {
                    if (dsType.equals(DbusDatasourceType.LOG_LOGSTASH)
                            || dsType.equals(DbusDatasourceType.LOG_LOGSTASH_JSON)) {
                        JSONObject value = JSON.parseObject(record.value());
                        for (String key : value.keySet()) {
                            if (value.get(key) instanceof String) continue;
                            value.put(key, JSON.toJSONString(value.get(key)));
                        }
                        ArrayList<String> values = new ArrayList<String>() {{
                            add(JSON.toJSONString(value));
                        }};
                        valuesList.add(values);
                    } else if (dsType.equals(DbusDatasourceType.LOG_FLUME) || dsType.equals(DbusDatasourceType.LOG_FILEBEAT) || dsType.equals(DbusDatasourceType.LOG_UMS)) {
                        ArrayList<String> values = new ArrayList<String>() {{
                            add(record.value());
                        }};
                        valuesList.add(values);
                    }

                    ArrayList<String> key = new ArrayList<String>() {{
                        add(record.key());
                    }};
                    keysList.add(key);

                    if (valuesList.size() >= count) break;
                }
                offset += records.count();
                consumer.seek(dataTopicPartition, offset);
                if (records.count() == 0) {
                    logger.warn("There is no content while reading kafka plain log");
                    break;
                }
            }
            Map<String, Object> ret = new HashMap<>();
            ret.put("keysList", keysList);
            ret.put("valuesList", valuesList);
            return ret;
        } catch (Exception e) {
            logger.error("Read kafka plain log error", e);
            return null;
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }

    public ResultEntity executeSql(ExecuteSqlBean executeSqlBean) {
        //根据dsId获取datasource信息
        Integer dsId = executeSqlBean.getDsId();
        String type = executeSqlBean.getType();
        String sql = executeSqlBean.getSql();
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/datasource/{id}", dsId);
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success()) {
            return result.getBody();
        }
        DataSource ds = result.getBody().getPayload(new TypeReference<DataSource>() {
        });

        //构造参数
        Map<String, Object> param = new HashedMap();
        param.put("dsType", ds.getDsType());
        param.put("sql", sql);
        param.put("user", ds.getDbusUser());
        param.put("password", ds.getDbusPwd());
        if (StringUtils.equals(type, "master")) {
            param.put("URL", ds.getMasterUrl());
        } else if (StringUtils.equals(type, "slave")) {
            param.put("URL", ds.getSlaveUrl());
        }

        //执行sql语句，返回结果
        result = sender.post(ServiceNames.KEEPER_SERVICE, "tables/execute-sql", param);
        return result.getBody();
    }

    public ResultEntity fetchEncodeAlgorithms() {
        return sender.get(ServiceNames.KEEPER_SERVICE, "encode-plugins/project-plugins/0").getBody();
    }

    public int countActiveTables(Integer tableId) {
        //是否还有项目在使用
        Integer count = sender.get(ServiceNames.KEEPER_SERVICE, "/projectTable/count-by-table-id/{0}", tableId).getBody().getPayload(Integer.class);
        DataTable table = this.getTableById(tableId);
        return count + (StringUtils.equals("ok", table.getStatus()) ? 1 : 0);
    }

    public List<RiderTable> riderSearch(Integer userId, String userRole) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<RiderTable> listRider = new ArrayList<RiderTable>();
        //管理员获取的是不带topoName的namespace
        if ("admin".equalsIgnoreCase(userRole)) {
            List<DataTable> tables = sender.get(ServiceNames.KEEPER_SERVICE, "/tables/findAllTables")
                    .getBody().getPayload(new TypeReference<List<DataTable>>() {
                    });
            boolean flag = false;
            for (DataTable table : tables) {
                if (table.getTableName().equals(table.getPhysicalTableRegex())) {
                    flag = true;
                }
                String dsType = table.getDsType();
                String namespace;
                if (flag) {
                    namespace = String.format("%s.%s.%s.%s.%s.%s.%s", dsType, table.getDsName(), table.getSchemaName(),
                            table.getTableName(), table.getVersion(), "0", "0");
                } else {
                    namespace = String.format("%s.%s.%s.%s.%s.%s.%s", dsType, table.getDsName(), table.getSchemaName(),
                            table.getTableName(), table.getVersion(), "0", table.getPhysicalTableRegex());
                }
                RiderTable rTable = new RiderTable();
                rTable.setNamespace(namespace);
                rTable.setTopic(table.getOutputTopic());
                rTable.setId(table.getId());
                rTable.setCreateTime(table.getCreateTime());
                Properties globalConf = zkService.getProperties(Constants.GLOBAL_PROPERTIES_ROOT);
                rTable.setKafka(globalConf.getProperty("bootstrap.servers"));
                listRider.add(rTable);
            }
        } else {
            //租户获取的是带topoName的namespace
            List<HashMap<String, Object>> list = sender.get(ServiceNames.KEEPER_SERVICE, "/tables/findTablesByUserId/{0}", userId)
                    .getBody().getPayload(new TypeReference<List<HashMap<String, Object>>>() {
                    });
            boolean flag = false;
            for (HashMap<String, Object> table : list) {
                if (table.get("table_name").equals(table.get("physical_table_regex"))) {
                    flag = true;
                }
                String dsType = (String) table.get("ds_type");
                String namespace;
                if (flag) {
                    namespace = String.format("%s.%s!%s.%s.%s.%s.%s.%s", dsType, table.get("ds_name"), table.get("topo_name"),
                            table.get("schema_name"), table.get("table_name"), table.get("version"), "0", "0");
                } else {
                    namespace = String.format("%s.%s!%s.%s.%s.%s.%s.%s", dsType, table.get("ds_name"), table.get("topo_name"),
                            table.get("schema_name"), table.get("table_name"), table.get("version"), "0", table.get("physical_table_regex"));
                }
                RiderTable rTable = new RiderTable();
                rTable.setNamespace(namespace);
                rTable.setTopic((String) table.get("output_topic"));
                rTable.setId((Integer) table.get("id"));
                rTable.setCreateTime(sdf.parse(table.get("create_time").toString()));
                rTable.setKafka((String) table.get("url"));
                listRider.add(rTable);
            }
        }
        return listRider;
    }

    public int rerun(Integer dsId, String dsName, String schemaName, String tableName, Long offset) throws Exception {
        String path = "/DBus/Topology/" + dsName + "-dispatcher/dispatcher.raw.topics.properties";
        byte[] data = zkService.getData(path);
        OrderedProperties orderedProperties = new OrderedProperties(new String(data));
        Object value = orderedProperties.get("dbus.dispatcher.offset");
        if (value != null && StringUtils.isNotBlank(value.toString()) && !"none".equals(value)) {
            return MessageCode.PLEASE_TRY_AGAIN_LATER;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(dsName).append(".").append(schemaName.toLowerCase()).append(".").append(tableName.toLowerCase()).append("->").append(offset);
        orderedProperties.put("dbus.dispatcher.offset", sb.toString());
        zkService.setData(path, orderedProperties.toString().getBytes());
        toolSetService.reloadConfig(dsId, dsName, "DISPATCHER_RELOAD_CONFIG");
        return 0;
    }

    public ResultEntity batchStartTableByTableIds(ArrayList<Integer> tableIds) {
        return startOrStopTableByTableIds(tableIds,"ok");
    }

    public ResultEntity batchStopTableByTableIds(ArrayList<Integer> tableIds) {
        return startOrStopTableByTableIds(tableIds,"abort");
    }

    private ResultEntity startOrStopTableByTableIds(ArrayList<Integer> tableIds,String status) {
        String query = "/tables/startOrStopTableByTableIds?status=" + status;
        ResponseEntity<ResultEntity> result = sender.post(ServiceNames.KEEPER_SERVICE, query, tableIds);
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success()) {
            logger.error("update table status by table id list error {}", tableIds);
            return result.getBody();
        }
        result = sender.post(ServiceNames.KEEPER_SERVICE, "/tables/getDataSourcesByTableIds", tableIds);
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success()) {
            logger.error("get datasource by table id list error {}", tableIds);
            return result.getBody();
        }
        List<Map<String, Object>> dataSources = result.getBody().getPayload(new TypeReference<List<Map<String, Object>>>() {
        });
        for (Map<String, Object> map : dataSources) {
            int i = toolSetService.reloadConfig((Integer) map.get("id"), (String) map.get("ds_name"), ToolSetService.APPENDER_RELOAD_CONFIG);
            if (i != 0) {
                logger.error("error when send APPENDER_RELOAD_CONFIG {}", map);
                ResultEntity resultEntity = new ResultEntity(0, null);
                resultEntity.setStatus(i);
                return resultEntity;
            }
        }
        logger.info("batch start tables by table id list success .tableids:{}", tableIds);
        return result.getBody();
    }

    public ResultEntity importRulesByTableId(Integer tableId, MultipartFile uploadFile) throws Exception{
        File saveDir = new File(SystemUtils.getJavaIoTmpDir(), String.valueOf(System.currentTimeMillis()));
        if (!saveDir.exists()) saveDir.mkdirs();
        File tempFile = new File(saveDir, uploadFile.getOriginalFilename());
        uploadFile.transferTo(tempFile);
        StringBuilder sb = new StringBuilder();
        BufferedReader br = null;
        try {
             br = new BufferedReader(new FileReader(tempFile));
            String line = null;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
        } finally {
            if (br != null) {
                br.close();
            }
            if (tempFile != null && tempFile.exists()) {
                tempFile.delete();
            }
        }
        return sender.post(ServiceNames.KEEPER_SERVICE, "/tables/importRulesByTableId/"+tableId, sb.toString()).getBody();
    }

    public void exportRulesByTableId(Integer tableId, HttpServletResponse response) {
        ResultEntity body = sender.get(ServiceNames.KEEPER_SERVICE, "/tables/exportRulesByTableId/{0}", tableId).getBody();
        String payload = body.getPayload(String.class);
        String fileName = System.currentTimeMillis() + ".json";
        response.setHeader("content-type", "application/octet-stream");
        response.setContentType("application/octet-stream");
        response.setHeader("Content-Disposition", "attachment; filename=" + fileName);
        OutputStream os = null;
        try {
            os = response.getOutputStream();
            os.write(payload.getBytes(KeeperConstants.UTF8));
            os.flush();
        } catch (IOException e) {
            logger.error("Exception when export rules by tableid {}", tableId);
        }
    }

    public static class InitialLoadStatus {
        private String status;
        @JSONField(format = "yyyy-MM-dd HH:mm:ss.SSS")
        private Date updateTime;

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public Date getUpdateTime() {
            return updateTime;
        }

        public void setUpdateTime(Date updateTime) {
            this.updateTime = updateTime;
        }
    }

    public static class ActiveTableParam {
        private String type;
        private int version;

        public static ActiveTableParam build(Map<String, String> map) {
            ActiveTableParam p = new ActiveTableParam();
            if (map == null) return p;
            if (map.containsKey("type")) {
                p.setType(map.get("type"));
            }
            if (map.containsKey("version")) {
                p.setVersion(Integer.parseInt(map.get("version")));
            }
            return p;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }

        public int getVersion() {
            return version;
        }

        public void setVersion(int version) {
            this.version = version;
        }
    }

    public ResultEntity findTables(String queryString) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/tables/find", queryString);
        return result.getBody();
    }

    public ResultEntity findAllTables(String queryString) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/tables/findAll", queryString);
        return result.getBody();
    }

    /*public ResultEntity findTables(Integer dsId,String schemaName,String tableName,Integer pageNum,Integer pageSize){
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/tables/find",dsId,schemaName,tableName,pageNum,pageSize);
        return result.getBody();
    }*/

    public ResultEntity getDSList() {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/tables/DSList");
        return result.getBody();
    }

    public ResultEntity findById(Integer tableId) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/tables/{tableId}", tableId);
        return result.getBody();
    }

    public DataTable findTableById(Integer tableId) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/tables/{tableId}", tableId);
        return result.getBody().getPayload(DataTable.class);
    }

    /**
     * 根据dsId 和 schemaName 查询，某scheme下的table,不分页
     *
     * @return
     */
    public ResultEntity findTablesToAdd(String queryString) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/tables/tables-to-add", queryString);
        return result.getBody();
    }

}
