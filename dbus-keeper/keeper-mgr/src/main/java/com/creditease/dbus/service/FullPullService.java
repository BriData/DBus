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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.base.com.creditease.dbus.utils.RequestSender;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.ControlMessage;
import com.creditease.dbus.commons.DbusMessage;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.constant.ServiceNames;
import com.creditease.dbus.domain.model.*;
import com.creditease.dbus.utils.ControlMessageSender;
import com.creditease.dbus.utils.ControlMessageSenderProvider;
import com.creditease.dbus.utils.JsonFormatUtils;
import com.creditease.dbus.utils.SecurityConfProvider;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Future;


/**
 * Created by xiancangao on 2018/04/27.
 */
@Service
public class FullPullService {

    @Autowired
    private IZkService zkService;
    @Autowired
    private RequestSender requestSender;
    @Autowired
    private TableService tableService;
    @Autowired
    private ProjectTableService projectTableService;
    @Autowired
    private ProjectService projectService;
    @Autowired
    private RequestSender sender;
    @Autowired
    private InitialLoadService initialLoadService;

    protected Logger logger = LoggerFactory.getLogger(getClass());

    private static final String MS_SERVICE = ServiceNames.KEEPER_SERVICE;

    private static final String FULL_PULL_REQUEST_FROM = "full-pull-request-from-project";
    private static final String FULL_PULL_TYPE = "FULL_DATA_INDEPENDENT_PULL_REQ";
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");


    public int sendMessage(DataTable table, String strMessage, FullPullHistory fullPullHistory) {

        String ctrlTopic = table.getCtrlTopic();
        try {
            ControlMessage controlMessage = ControlMessage.parse(strMessage);
            if (!validate(controlMessage)) {
                logger.error("[control message] controlMessage validated error.\ncontrol ctrlTopic:{}\nmessage:{}", ctrlTopic, strMessage);
                return MessageCode.ILLEGAL_CONTROL_MESSAGE;
            }
            Map<String, Object> payload = controlMessage.getPayload();
            String resultTopic = payload.get("resultTopic") == null ? null : payload.get("resultTopic").toString();

             /*
             对于mysql
             需要去源端查询physical_table_regex中的内容
             */
            if (StringUtils.equalsIgnoreCase(table.getDsType(), "mysql")) {
                Class.forName("com.mysql.jdbc.Driver");
                Connection conn = DriverManager.getConnection(table.getMasterUrl(), table.getDbusUser(), table.getDbusPassword());
                String physicalTables = initialLoadService.getMysqlTables(conn, table);
                conn.close();
                payload.put("PHYSICAL_TABLES", physicalTables);
            } else if (StringUtils.equalsIgnoreCase(table.getDsType(), "oracle")) {
                Class.forName("oracle.jdbc.driver.OracleDriver");
                Connection conn = DriverManager.getConnection(table.getMasterUrl(), table.getDbusUser(), table.getDbusPassword());
                String physicalTables = initialLoadService.getOracleTables(conn, table);
                conn.close();
                payload.put("PHYSICAL_TABLES", physicalTables);
            }

            payload = this.setOPTS(table.getOutputTopic(), payload);
            controlMessage.setPayload(payload);

            //解决一毫秒处理多个全量任务,导致数据库任务缺失.原因:FullPullHistory主键是时间戳
            long time = System.currentTimeMillis();
            FullPullHistory history = requestSender.get(MS_SERVICE, "/fullPullHistory/getById/{id}", time).getBody().getPayload(FullPullHistory.class);
            if (history != null) {
                Long maxId = requestSender.get(MS_SERVICE, "/fullPullHistory/getMaxId").getBody().getPayload(Long.class);
                time = maxId + 1;
            }

            //原来的时间是前端获取的操作者的机器时间戳,
            //不同机器时间不一致会导致会导致:t_fullpull_history表的id排序和kafka topic full_pull_req_msg_offset排序不一致的问题
            Date date = new Date(time);
            String timestamp = sdf.format(date);
            payload.put("SEQNO", time);
            controlMessage.setPayload(payload);
            controlMessage.setId(time);
            controlMessage.setTimestamp(timestamp);
            fullPullHistory.setId(time);

            ControlMessageSender sender = ControlMessageSenderProvider.getControlMessageSender(zkService);
            controlMessage.addPayload("OGG_OP_TS", payload.get("OP_TS"));

            requestSender.post(MS_SERVICE, "/fullPullHistory/create", fullPullHistory);

            synchronized (this.getClass()) {
                long positionOffset = sender.send(ctrlTopic, controlMessage);
                fullPullHistory.setFullPullReqMsgOffset(positionOffset);
                fullPullHistory.setTargetSinkTopic(resultTopic);
                requestSender.post(MS_SERVICE, "/fullPullHistory/update", fullPullHistory);
                logger.info("[control message] Send control message to ctrlTopic[{}] success.\n message.pos: {} \n message.op_ts: {}",
                        ctrlTopic, controlMessage.getPayload().get("POS"), controlMessage.getPayload().get("OP_TS"));
            }
            return 0;
        } catch (Exception e) {
            logger.error("[control message] Error encountered while sending control message.\ncontrol ctrlTopic:{}\n message:{}", ctrlTopic, strMessage, e);
            return MessageCode.EXCEPTION_WHEN_GOLBAL_FULLPULL;
        }
    }

    public JSONObject buildProject(ProjectTopoTable topoTable, String projectName) {
        JSONObject project = new JSONObject();
        project.put("id", topoTable.getProjectId());
        project.put("name", projectName);
        project.put("sink_id", topoTable.getSinkId());
        project.put("topo_table_id", topoTable.getId());

        return project;
    }

    public JSONObject buildPayload(DataTable dataTable, Long time, String resultTopic, String hdfsRootPath) {
        JSONObject payload = new JSONObject();
        payload.put("DBUS_DATASOURCE_ID", String.valueOf(dataTable.getDsId()));
        payload.put("SCHEMA_NAME", dataTable.getSchemaName());
        payload.put("TABLE_NAME", dataTable.getTableName());
        payload.put("INCREASE_VERSION", "false");
        payload.put("INCREASE_BATCH_NO", "false");
        payload.put("SEQNO", String.valueOf(time));
        payload.put("PHYSICAL_TABLES", dataTable.getPhysicalTableRegex());
        payload.put("PULL_REMARK", "");
        payload.put("SPLIT_BOUNDING_QUERY", "");
        payload.put("PULL_TARGET_COLS", "");
        payload.put("SCN_NO", "");
        payload.put("SPLIT_COL", dataTable.getFullpullCol());
        payload.put("SPLIT_SHARD_SIZE", dataTable.getFullpullSplitShardSize());
        payload.put("SPLIT_SHARD_STYLE", dataTable.getFullpullSplitStyle());
        payload.put("INPUT_CONDITIONS", dataTable.getFullpullCondition());
        // 处理sink
        if (StringUtils.isNotBlank(hdfsRootPath)) {
            logger.info("全量任务为直接落hdfs:{}", hdfsRootPath);
            payload.put(KeeperConstants.FULL_PULL_PAYLOAD_SINK_TYPE, "HDFS");
            payload.put(KeeperConstants.FULL_PULL_PAYLOAD_HDFS_ROOT_PATH, hdfsRootPath);
        } else {
            if (StringUtils.isBlank(resultTopic)) {
                resultTopic = String.format("%s.%s.%s", dataTable.getDsType(), dataTable.getSchemaName(), System.currentTimeMillis());
            }
            payload.put("resultTopic", resultTopic);
            payload.put(KeeperConstants.FULL_PULL_PAYLOAD_SINK_TYPE, "KAFKA");
        }
        return payload;
    }

    public JSONObject buildMessage(Date date) {
        JSONObject message = new JSONObject();
        long time = date.getTime();
        message.put("from", FULL_PULL_REQUEST_FROM);
        message.put("type", FULL_PULL_TYPE);
        message.put("id", time);
        message.put("timestamp", sdf.format(date));
        return message;
    }


    private int findDbusMessageFieldIndex(JSONObject jsonDbusMessage, String string) {
        try {
            JSONObject jsonSchema = jsonDbusMessage.getJSONObject("schema");
            JSONArray jsonFields = jsonSchema.getJSONArray("fields");
            for (int i = 0; i < jsonFields.size(); i++) {
                if (jsonFields.getJSONObject(i).getString("name").equals(string)) return i;
            }
        } catch (Exception e) {
            logger.error("[control message] Parsing dbusmessage to json failed, message: {}", e);
        }
        return -1;
    }

    private boolean validate(ControlMessage message) {
        if (message.getId() <= 0) return false;
        if (StringUtils.isBlank(message.getType())) return false;
        if (StringUtils.isBlank(message.getFrom())) return false;
        if (StringUtils.isBlank(message.getTimestamp())) return false;
        if (message.getPayload().isEmpty()) return false;
        return true;
    }

    public ResultEntity updateFullpullCondition(ProjectTopoTable topoTable) {
        return requestSender.post(MS_SERVICE, "/fullpull/updateCondition", topoTable).getBody();
    }

    public int globalfullPull(Integer topoTableId, Integer tableId, String outputTopic, String splitShardSize,
                              String splitCol, String splitShardStyle, String inputConditions, String hdfsRootPath) throws Exception {
        int result = 0;
        JSONObject message = null;
        DataTable dataTable = null;
        ProjectTopoTable topoTable = null;

        if (topoTableId != null && topoTableId != 0) {
            topoTable = projectTableService.getTableById(topoTableId);
            if (topoTable == null) {
                logger.error("[globalfullPull] can not find topotable by topoTableId {}", topoTableId);
                return MessageCode.TABLE_NOT_FOUND;
            }
            dataTable = tableService.findTableById(topoTable.getTableId());
            if (dataTable == null) {
                logger.error("[globalfullPull] can not find table by tableId {}", tableId);
                return MessageCode.TABLE_NOT_FOUND_BY_ID;
            }
            result = hashFullPullTopo(dataTable.getDsName());
            if (result != 0) {
                return result;
            }
            result = isFullpullEnable(topoTable.getTableId(), topoTable.getProjectId());
            if (result != 0) {
                return result;
            }
            message = buildProjectFullPullMessage(topoTable, dataTable, outputTopic);
        } else {
            dataTable = tableService.findTableById(tableId);
            if (dataTable == null) {
                logger.error("[globalfullPull] can not find table by tableId {}", tableId);
                return MessageCode.TABLE_NOT_FOUND_BY_ID;
            }
            result = hashFullPullTopo(dataTable.getDsName());
            if (result != 0) {
                return result;
            }
            message = buildSourceFullPullMessage(dataTable, outputTopic, hdfsRootPath);
        }
        JSONObject payload = message.getJSONObject("payload");
        if (StringUtils.isNotBlank(splitShardSize)) {
            payload.put("SPLIT_SHARD_SIZE", splitShardSize);
        }
        if (StringUtils.isNotBlank(splitCol)) {
            payload.put("SPLIT_COL", splitCol);
        }
        if (StringUtils.isNotBlank(splitShardStyle)) {
            payload.put("SPLIT_SHARD_STYLE", splitShardStyle);
        }
        if (StringUtils.isNotBlank(inputConditions)) {
            payload.put("INPUT_CONDITIONS", inputConditions);
        }
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
        fullPullHistory.setFullpullCondition(inputConditions);
        if (topoTableId != null && topoTableId != 0) {
            JSONObject projectJson = message.getJSONObject("project");
            fullPullHistory.setProjectName(projectJson.getString("name"));
            fullPullHistory.setTopologyTableId(topoTable.getId());
            fullPullHistory.setTargetSinkId(topoTable.getSinkId());
        }

        //发送消息
        result = this.sendMessage(dataTable, message.toJSONString(), fullPullHistory);
        if (0 != result) {
            return result;
        }
        return result;
    }

    /**
     * 全量topo是否启动
     *
     * @param dsName
     * @return
     * @throws Exception
     */
    public int hashFullPullTopo(String dsName) throws Exception {
//        if (stormTopoHelper.getTopologyByName(dsName + "-splitter-puller") == null) {
//            return MessageCode.FULLPULL_TOPO_IS_NOT_RUNNING;
//        }
        return 0;
    }

    /**
     * 根据Resource配置判断全量开关是否打开
     *
     * @param topoTableId
     * @param projectId
     * @return
     */
    public int isFullpullEnable(Integer topoTableId, Integer projectId) {
        ProjectResource resource = sender.get(ServiceNames.KEEPER_SERVICE, "/projectResource/{0}/{1}",
                projectId, topoTableId).getBody().getPayload(new TypeReference<ProjectResource>() {
        });
        if (resource.getFullpullEnableFlag() == ProjectResource.FULL_PULL_ENABLE_FALSE) {
            logger.warn("[globalfullPull] fullPullEnable of resource is false . projectResourceId:{},topoTableId:{}", resource.getId(), topoTableId);
            return MessageCode.TABLE_RESOURCE_FULL_PULL_FALSE;
        } else {
            return 0;
        }
    }

    public JSONObject buildSourceFullPullMessage(DataTable dataTable, String outputTopic, String hdfsRootPath) {
        Date date = new Date();
        JSONObject message = buildMessage(date);
        JSONObject payloadJson = buildPayload(dataTable, date.getTime(), outputTopic, hdfsRootPath);
        message.put("payload", payloadJson);
        return message;
    }

    public JSONObject buildProjectFullPullMessage(ProjectTopoTable topoTable, DataTable dataTable, String outputTopic) throws Exception {
        Project project = projectService.queryProjectId(topoTable.getProjectId()).getPayload(Project.class);
        //安全模式,如果需要新建topic,要插入acl
        if (StringUtils.equals(SecurityConfProvider.getSecurityConf(zkService), Constants.SECURITY_CONFIG_TRUE_VALUE)) {
            projectTableService.addAclTopic(outputTopic, project.getPrincipal());
        }
        Date date = new Date();
        JSONObject message = this.buildMessage(date);
        JSONObject payloadJson = this.buildPayload(dataTable, date.getTime(), outputTopic, null);
        JSONObject projectJson = this.buildProject(topoTable, project.getProjectName());
        message.put("payload", payloadJson);
        message.put("project", projectJson);
        return message;
    }

    public Map<String, Object> setOPTS(String outputTopic, Map<String, Object> payload) throws Exception {
        String key;
        String value;
        final String OP_TS = "OP_TS";

        logger.info("outputTopic:{},payload:{}", outputTopic, payload);
        if (payload.get("POS") != null && payload.get(OP_TS) != null) {
            logger.info("skip this step ,because POS and OP_TS is not null");
            return payload;
        }

        Properties consumerProps = zkService.getProperties(KeeperConstants.KEEPER_CONSUMER_CONF);
        consumerProps.setProperty("client.id", "full-pull.reader");
        consumerProps.setProperty("group.id", "full-pull.reader");
        Properties globalConf = zkService.getProperties(KeeperConstants.GLOBAL_CONF);
        consumerProps.setProperty(KeeperConstants.GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS, globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS));
        if (StringUtils.equals(SecurityConfProvider.getSecurityConf(zkService), Constants.SECURITY_CONFIG_TRUE_VALUE)) {
            consumerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        }


        KafkaConsumer<String, byte[]> consumer = null;
        try {
            consumer = new KafkaConsumer(consumerProps);
            TopicPartition dataTopicPartition = new TopicPartition(outputTopic, 0);
            List<TopicPartition> topics = Arrays.asList(dataTopicPartition);
            consumer.assign(topics);
            consumer.seekToBeginning(topics);
            long beginOffset = consumer.position(dataTopicPartition);
            consumer.seekToEnd(topics);
            long offset = consumer.position(dataTopicPartition);

            long step = Integer.valueOf(consumerProps.getProperty("max.poll.records"));
            boolean runing = true;
            while (runing) {
                ConsumerRecords<String, byte[]> results = consumer.poll(100);
                while (results.isEmpty()) {
                    offset = offset - step;
                    if (offset < beginOffset) {
                        logger.info("没有找到 op_ts .");
                        payload.put("POS", 0);
                        payload.put(OP_TS, DateFormatUtils.format(Calendar.getInstance(), "yyyy-MM-dd HH:mm:ss.SSS"));
                        runing = false;
                        break;
                    }
                    consumer.seek(dataTopicPartition, offset);
                    results = consumer.poll(100);
                }
                for (ConsumerRecord record : results) {
                    key = record.key().toString();
                    value = record.value().toString();
                    JSONObject jsonDbusMessage = JSON.parseObject(value);
                    int iPos = findDbusMessageFieldIndex(jsonDbusMessage, DbusMessage.Field._UMS_ID_);
                    int iOpTs = findDbusMessageFieldIndex(jsonDbusMessage, DbusMessage.Field._UMS_TS_);
                    if (iPos == -1 || iOpTs == -1) continue;
                    if (key.indexOf("data_increment_heartbeat") != -1 || key.indexOf("data_increment_data") != -1) {
                        JSONArray tuple = jsonDbusMessage.getJSONArray("payload").getJSONObject(0).getJSONArray("tuple");
                        String pos = tuple.getString(iPos);
                        String op_ts = tuple.getString(iOpTs);
                        logger.info("找到了topic:{}, offset:{}, op_ts, pos : {} ,op_ts : {},{}", outputTopic, record.offset(), pos, op_ts, tuple);
                        payload.put("POS", pos);
                        payload.put(OP_TS, op_ts);
                        runing = false;
                        break;
                    }
                }
            }
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
        return payload;
    }

    public ResultEntity updateSourceFullpullCondition(DataTable dataTable) {
        return requestSender.post(MS_SERVICE, "/fullpull/updateSourceFullpullCondition", dataTable).getBody();
    }

    public ResultEntity resumingFullPull(Long id) throws Exception {
        FullPullHistory fullPullHistory = sender.get(ServiceNames.KEEPER_SERVICE, "/fullPullHistory/getById/{id}", id).getBody().getPayload(FullPullHistory.class);
        ResultEntity resultEntity = new ResultEntity();

        String dsName = fullPullHistory.getDsName();
        Long currentShardOffset = fullPullHistory.getCurrentShardOffset();
        Long firstShardOffset = fullPullHistory.getFirstShardMsgOffset();
        Long lastShardOffset = fullPullHistory.getLastShardMsgOffset();
        if (firstShardOffset == null || lastShardOffset == null) {
            resultEntity.setStatus(MessageCode.EXCEPTION);
            resultEntity.setMessage("分片未完成不能进行断点续传.");
        }
        boolean resumingAll = false;
        if (currentShardOffset == null) {
            currentShardOffset = firstShardOffset;
            fullPullHistory.setFinishedPartitionCount(null);
            fullPullHistory.setFinishedRowCount(null);
            resumingAll = true;
        }
        fullPullHistory.setState("pulling");
        fullPullHistory.setErrorMsg("");
        sender.post(ServiceNames.KEEPER_SERVICE, "/fullPullHistory/update", fullPullHistory);

        String mediantTopic = dsName + "_data_shards";
        TopicPartition dataTopicPartition = new TopicPartition(mediantTopic, 0);
        List<TopicPartition> topics = Arrays.asList(dataTopicPartition);

        Properties consumerProps = zkService.getProperties(KeeperConstants.KEEPER_CONSUMER_CONF);
        consumerProps.setProperty("client.id", String.valueOf(System.currentTimeMillis()));
        consumerProps.setProperty("group.id", "dbus-fullpull.reader");
        Properties globalConf = zkService.getProperties(KeeperConstants.GLOBAL_CONF);
        consumerProps.setProperty(KeeperConstants.GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS, globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS));

        Properties producerConf = zkService.getProperties(KeeperConstants.KEEPER_CTLMSG_PRODUCER_CONF);
        producerConf.setProperty(KeeperConstants.GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS, globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS));
        if (StringUtils.equals(SecurityConfProvider.getSecurityConf(zkService), Constants.SECURITY_CONFIG_TRUE_VALUE)) {
            consumerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            producerConf.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        }
        KafkaConsumer<String, String> consumer = null;
        KafkaProducer<String, byte[]> producer = null;
        JSONObject reqJson = null;
        Long splitIndex = null;
        ArrayList<String> list = new ArrayList<>();
        try {
            producer = new KafkaProducer<>(producerConf);
            consumer = new KafkaConsumer(consumerProps);
            consumer.assign(topics);
            consumer.seekToEnd(topics);
            long endOffset = consumer.position(dataTopicPartition);
            consumer.seek(dataTopicPartition, currentShardOffset);

            //查找需要回灌的分片任务
            boolean running = true;
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                if (records == null || records.count() == 0) {
                    break;
                }
                for (ConsumerRecord<String, String> record : records) {
                    String value = record.value();
                    JSONObject jsonObject = JSON.parseObject(value);
                    JSONObject request = JSON.parseObject(jsonObject.getString("fullPull.request.param"));
                    Long seqNo = request.getJSONObject("payload").getLong("SEQNO");
                    if (id.equals(seqNo)) {
                        list.add(value);
                    }
                    if (currentShardOffset.equals(record.offset())) {
                        reqJson = request;
                        splitIndex = jsonObject.getLong("dataChunkSplitIndex");
                    }
                    if (record.offset() > lastShardOffset || record.offset() >= (endOffset - 1)) {
                        running = false;
                        break;
                    }
                }
            }
            //处理监控节点
            String monitorNodePath = getMonitorNodePath(dsName, reqJson);
            LinkedHashMap<String, Object> monitorData = getFullPullMonitorData(monitorNodePath);
            monitorData.put("FinishedCount", (--splitIndex).toString());
            monitorData.put("Status", "pulling");
            monitorData.put("ErrorMsg", null);
            monitorData.put("EndTime", null);
            if (resumingAll) {
                monitorData.put("FinishedRows", "0");
            }
            updateMonitor(monitorNodePath, monitorData);
            //回灌分片任务
            for (int i = 0; i < list.size(); i++) {
                String value = list.get(i);
                Future<RecordMetadata> result = producer.send(new ProducerRecord<>(mediantTopic, "FULL_DATA_INDEPENDENT_PULL_REQ", value.getBytes()), null);
                RecordMetadata recordMetadata = result.get();
                if (i == 0) {
                    fullPullHistory.setFirstShardMsgOffset(recordMetadata.offset());
                }
                if (i == (list.size() - 1)) {
                    fullPullHistory.setLastShardMsgOffset(recordMetadata.offset());
                }
            }

            sender.post(ServiceNames.KEEPER_SERVICE, "/fullPullHistory/update", fullPullHistory);
            logger.info("断点续传更新分片信息到数据库,{}", JSON.toJSONString(fullPullHistory));
            return resultEntity;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        } finally {
            if (consumer != null) {
                consumer.close();
            }
            if (producer != null) {
                producer.close();
            }
        }
    }

    public ResultEntity rerun(Long id) throws Exception {
        ResultEntity resultEntity = new ResultEntity();
        FullPullHistory fullPullHistory = sender.get(ServiceNames.KEEPER_SERVICE, "/fullPullHistory/getById/{id}", id).getBody().getPayload(FullPullHistory.class);
        if (fullPullHistory.getFullPullReqMsgOffset() == null) {
            resultEntity.setStatus(MessageCode.EXCEPTION);
            resultEntity.setMessage("任务拉取信息offset不能为空");
            return resultEntity;
        }
        String dsName = fullPullHistory.getDsName();
        logger.info("全量任务重跑 {}", JSON.toJSONString(fullPullHistory));

        fullPullHistory.setState("init");
        fullPullHistory.setErrorMsg("");
        fullPullHistory.setTotalPartitionCount(0L);
        fullPullHistory.setTotalRowCount(0L);
        fullPullHistory.setCurrentShardOffset(0L);
        fullPullHistory.setFirstShardMsgOffset(0L);
        fullPullHistory.setLastShardMsgOffset(0L);
        fullPullHistory.setFinishedPartitionCount(0L);
        fullPullHistory.setFinishedRowCount(0L);
        sender.post(ServiceNames.KEEPER_SERVICE, "/fullPullHistory/update", fullPullHistory);
        logger.info("全量任务重跑更新到数据库,{}", JSON.toJSONString(fullPullHistory));

        String ctrlTopic = dsName + "_ctrl";
        TopicPartition dataTopicPartition = new TopicPartition(ctrlTopic, 0);
        List<TopicPartition> topics = Arrays.asList(dataTopicPartition);

        Properties consumerProps = zkService.getProperties(KeeperConstants.KEEPER_CONSUMER_CONF);
        consumerProps.setProperty("client.id", String.valueOf(System.currentTimeMillis()));
        consumerProps.setProperty("group.id", "dbus-fullpull.reader");
        Properties globalConf = zkService.getProperties(KeeperConstants.GLOBAL_CONF);
        consumerProps.setProperty(KeeperConstants.GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS, globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS));

        Properties producerConf = zkService.getProperties(KeeperConstants.KEEPER_CTLMSG_PRODUCER_CONF);
        producerConf.setProperty(KeeperConstants.GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS, globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS));
        if (StringUtils.equals(SecurityConfProvider.getSecurityConf(zkService), Constants.SECURITY_CONFIG_TRUE_VALUE)) {
            consumerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            producerConf.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        }
        KafkaConsumer<String, String> consumer = null;
        KafkaProducer<String, byte[]> producer = null;
        ArrayList<String> list = new ArrayList<>();
        try {
            producer = new KafkaProducer<>(producerConf);
            consumer = new KafkaConsumer(consumerProps);
            consumer.assign(topics);
            consumer.seek(dataTopicPartition, fullPullHistory.getFullPullReqMsgOffset());
            ConsumerRecords<String, String> records = consumer.poll(1000);
            if (records.isEmpty()) {
                resultEntity.setStatus(MessageCode.EXCEPTION);
                resultEntity.setMessage("任务拉取信息offset已过期.");
                return resultEntity;
            }
            String key = null;
            String value = null;
            JSONObject reqJson = null;
            for (ConsumerRecord<String, String> record : records) {
                if (record.offset() == fullPullHistory.getFullPullReqMsgOffset()) {
                    key = record.key();
                    value = record.value();
                    reqJson = JSONObject.parseObject(record.value());
                }
            }
            //处理监控节点
            String monitorNodePath = getMonitorNodePath(dsName, reqJson);
            LinkedHashMap<String, Object> monitorData = getFullPullMonitorData(monitorNodePath);
            monitorData.put("FinishedCount", null);
            monitorData.put("FinishedRows", null);
            monitorData.put("EndTime", null);
            monitorData.put("ErrorMsg", null);
            monitorData.put("Status", "init");
            monitorData.put("SplitStatus", null);
            updateMonitor(monitorNodePath, monitorData);
            //回灌分片任务
            Future<RecordMetadata> result = producer.send(new ProducerRecord<>(ctrlTopic, key, value.getBytes()), null);
            RecordMetadata recordMetadata = result.get();
            logger.info("全量任务重跑:offset:{} ,key:{},value:{}", recordMetadata.offset(), key, value);
            return resultEntity;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        } finally {
            if (consumer != null) {
                consumer.close();
            }
            if (producer != null) {
                producer.close();
            }
        }
    }

    private void updateMonitor(String monitorNodePath, LinkedHashMap<String, Object> monitorData) throws Exception {
        String format = JsonFormatUtils.toPrettyFormat(JSON.toJSONString(monitorData, SerializerFeature.WriteMapNullValue));
        zkService.setData(monitorNodePath, format.getBytes(KeeperConstants.UTF8));
    }

    private LinkedHashMap<String, Object> getFullPullMonitorData(String monitorNodePath) throws Exception {
        byte[] data = zkService.getData(monitorNodePath);
        LinkedHashMap<String, Object> monitorData = JSON.parseObject(new String(data, KeeperConstants.UTF8),
                new com.alibaba.fastjson.TypeReference<LinkedHashMap<String, Object>>() {
                }, Feature.OrderedField);
        return monitorData;
    }


    private String getMonitorNodePath(String dsName, JSONObject reqJson) {
        String monitorRoot = Constants.FULL_PULL_MONITOR_ROOT;
        String dbNameSpace = buildSlashedNameSpace(dsName, reqJson);
        String monitorNodePath;
        JSONObject projectJson = reqJson.getJSONObject("project");
        if (projectJson != null && !projectJson.isEmpty()) {
            int projectId = projectJson.getIntValue("id");
            String projectName = projectJson.getString("name");
            monitorNodePath = String.format("%s/%s/%s_%s/%s", monitorRoot, "Projects", projectName, projectId, dbNameSpace);
        } else {
            monitorNodePath = buildZkPath(monitorRoot, dbNameSpace);
        }
        return monitorNodePath;
    }

    private String buildSlashedNameSpace(String dsName, JSONObject reqJson) {
        JSONObject payloadJson = reqJson.getJSONObject("payload");
        Long id = payloadJson.getLong("SEQNO");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss.SSS");
        String timestamp = sdf.format(new Date(id));
        String dbSchema = payloadJson.getString("SCHEMA_NAME");
        String tableName = payloadJson.getString("TABLE_NAME");
        return String.format("%s/%s/%s/%s - %s", dsName, dbSchema, tableName, timestamp, 0);
    }

    private String buildZkPath(String parent, String child) {
        return parent + "/" + child;
    }

}
