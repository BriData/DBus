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


package com.creditease.dbus.spout;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.common.FullPullConstants;
import com.creditease.dbus.common.bean.DBConfiguration;
import com.creditease.dbus.common.bean.ProgressInfo;
import com.creditease.dbus.common.bean.ProgressInfoParam;
import com.creditease.dbus.commons.ZkService;
import com.creditease.dbus.commons.exception.InitializationException;
import com.creditease.dbus.helper.DBHelper;
import com.creditease.dbus.helper.FullPullHelper;
import com.creditease.dbus.utils.TimeUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;

/**
 * 全量分片Spout
 * 读取kafka数据(ctrl_topic),包含全量reload和全量请求消息
 */
public class DataShardsSplittingSpout extends BaseRichSpout {
    private static final long serialVersionUID = -4784988717494537483L;
    private Logger logger = LoggerFactory.getLogger(getClass());

    private final String zkTopoRoot = FullPullConstants.TOPOLOGY_ROOT + "/" + FullPullConstants.FULL_SPLITTING_PROPS_ROOT;
    private Map confMap;
    private boolean isGlobal;
    private SpoutOutputCollector collector;
    final private int EMPTY_RUN_COUNT = 60000;
    private int suppressLoggingCount = EMPTY_RUN_COUNT / 2;
    private String zkConnect;
    private String topologyId;
    private String dsName;
    private Properties commonProps;

    private Consumer<String, byte[]> consumer;
    private ZkService zkService;

    /**
     * 初始化collector
     */
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.zkConnect = (String) conf.get(FullPullConstants.ZKCONNECT);
        this.topologyId = (String) conf.get(FullPullConstants.FULL_SPLITTER_TOPOLOGY_ID);
        this.dsName = (String) conf.get(FullPullConstants.DS_NAME);
        this.isGlobal = topologyId.toLowerCase().indexOf(FullPullConstants.GLOBAL_FULLPULLER_TOPO_PREFIX) != -1 ? true : false;
        logger.info("[split spout] topologyId:{} zkConnect:{} zkTopoRoot:{}", topologyId, zkConnect, zkTopoRoot);
        try {
            //设置topo类型，用于获取配置信息路径
            FullPullHelper.setTopologyType(FullPullConstants.FULL_SPLITTER_TYPE);
            loadRunningConf(null);

            //数据库pending任务处理
            FullPullHelper.updatePendingTasksToHistoryTable(dsName, FullPullConstants.FULLPULL_PENDING_TASKS_OP_SPLIT_TOPOLOGY_RESTART,
                    consumer, commonProps.getProperty(FullPullConstants.FULL_PULL_SRC_TOPIC));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new InitializationException();
        }
        logger.info("[split spout] {} init complete!", topologyId);
    }

    /**
     * 在SpoutTracker类中被调用，每调用一次就可以向storm集群中发射一条数据（一个tuple元组），该方法会被不停的调用
     */
    @Override
    public void nextTuple() {
        try {
            ConsumerRecords<String, byte[]> records = consumer.poll(0);
            // 判断是否有数据被读取
            if (records.isEmpty()) {
                if (canPrintNow()) {
                    logger.info("[split spout] running ...");
                }
                return;
            }
            // 按记录进行处理
            for (ConsumerRecord<String, byte[]> record : records) {
                String key = record.key();
                if (null == key) {
                    continue;
                }
                String reqString = new String(record.value());
                try {
                    if (key.equals(FullPullConstants.COMMAND_FULL_PULL_RELOAD_CONF)) {
                        logger.info("[split spout] receive config reloading request offset:{},key:{},value:{}", record.offset(), record.key(), reqString);
                        destory();
                        loadRunningConf(reqString);
                        collector.emit(FullPullConstants.CTRL_STREAM, new Values(reqString));
                        logger.info("[split spout] config for splitting spout is reloaded successfully!");
                    } else if (key.equals(FullPullConstants.DATA_EVENT_INDEPENDENT_FULL_PULL_REQ)) {
                        logger.info("[split spout] 收到全量分片任务: topic:{}, offset:{}, key:{}, value:{}", record.topic(), record.offset(), record.key(), reqString);
                        JSONObject reqJson = JSONObject.parseObject(reqString);
                        //判断能否继续分片(上一个任务状态必须是完成(ending/abort)状态才可以开始新的分片),保证同一时刻只有同一个任务在进行
                        //如果等待的这条任务由于其他原因状态异常,可以在web手动更改启状态为完成状态即可
                        waitForLastTaskToEnd(record.offset(), reqJson);
                        //每次拉取都要进行批次号加1处理,这条语句的位置不要变动
                        reqString = DBHelper.increseBatchNo(reqJson);
                        //创建zk节点并在zk节点上输出拉取信息
                        startSplitReport(zkService, reqString);
                        collector.emit(new Values(reqString), record);
                    } else {
                        logger.info("[split spout] unsupported command .topic:{},offset:{},key:{},value:{}", record.topic(), record.offset(), record.key(), reqString);
                    }
                } catch (Exception e) {
                    String errorMsg = "[split spout] spout-->bolt exception!" + e.getMessage();
                    logger.error(errorMsg, e);
                    if (key.equals(FullPullConstants.DATA_EVENT_INDEPENDENT_FULL_PULL_REQ)) {
                        FullPullHelper.finishPullReport(reqString, FullPullConstants.FULL_PULL_STATUS_ABORT, errorMsg);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Exception happended on split spout nextTuple()", e);
        }
    }

    /**
     * 只能有一个任务处于splitting,puliing的状态
     * 阻断增量的拉全量暂时没有好的处理办法,可能会有normal和indepent的任务同时处于splitting和pulling
     */
    private void waitForLastTaskToEnd(long offset, JSONObject reqJson) throws Exception {
        Long id = FullPullHelper.getSeqNo(reqJson);
        Connection conn = null;
        PreparedStatement pst = null;
        ResultSet ret = null;
        try {
            conn = DBHelper.getDBusMgrConnection();
            StringBuilder sql = new StringBuilder();

            sql.append(" SELECT  p.id ,p.full_pull_req_msg_offset,p.update_time FROM ");
            sql.append("( SELECT  h.id ,h.full_pull_req_msg_offset,h.state,h.update_time FROM t_fullpull_history h ");
            if (isGlobal) {
                sql.append(" WHERE h.type = 'global'");
            } else {
                sql.append(" WHERE h.type = 'indepent' and h.dsName = '").append(dsName).append("'");
            }
            sql.append(" and h.id < ").append(id).append(" and h.full_pull_req_msg_offset is not NULL ");
            sql.append(" ORDER BY h.id DESC LIMIT 1 )p ").append(" where p.state not in ('ending','abort')");

            logger.info("[split spout] select last task sql: {}", sql.toString());
            while (true) {
                pst = conn.prepareStatement(sql.toString());
                ret = pst.executeQuery();
                if (ret.next()) {
                    //20分钟没有更新直接开始下一个任务
                    long update_time = ret.getTimestamp("update_time").getTime();
                    if ((System.currentTimeMillis() - update_time) > 20 * 60 * 1000) {
                        return;
                    }
                    logger.info("[split spout] task id {} offset:{} is waitting for last task to end . lask task -> id:{} offset:{}", id, offset,
                            ret.getLong("id"), ret.getLong("full_pull_req_msg_offset"));
                    TimeUtils.sleep(60000);
                } else {
                    return;
                }
            }
        } catch (Exception e) {
            logger.error("Exception when wait for last task to end ", e);
            throw e;
        } finally {
            DBHelper.close(conn, pst, ret);
        }
    }

    @Override
    public void ack(Object msgId) {
        try {
            if (msgId != null && ConsumerRecord.class.isInstance(msgId)) {
                ConsumerRecord<String, byte[]> record = getMessageId(msgId);
                logger.info("[split ack] topic: {}, offset: {}, key: {}", record.topic(), record.offset(), record.key());
            }
            super.ack(msgId);
        } catch (Exception e) {
            logger.error("[split ack] error.", e);
        }
    }

    @Override
    public void fail(Object msgId) {
        try {
            if (msgId != null && ConsumerRecord.class.isInstance(msgId)) {
                ConsumerRecord<String, byte[]> record = getMessageId(msgId);
                logger.info("[split fail] topic: {}, offset: {}, key: {}", record.topic(), record.offset(), record.key());
            }
            super.fail(msgId);
        } catch (Exception e) {
            logger.error("[split fail] error.", e);
        }
    }

    /**
     * 定义字段id，该id在简单模式下没有用处，但在按照字段分组的模式下有很大的用处。
     * 该declarer变量有很大作用，我们还可以调用declarer.declareStream();来定义stramId，
     * 该id可以用来定义更加复杂的流拓扑结构
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(FullPullConstants.CTRL_STREAM, new Fields("source"));
        declarer.declare(new Fields("source"));
    }

    private <T> T getMessageId(Object msgId) {
        return (T) msgId;
    }

    public void startSplitReport(ZkService zkService, String reqString) throws Exception {
        JSONObject reqJson = JSONObject.parseObject(reqString);
        String currentTimeStampString = TimeUtils.getCurrentTimeStampString();
        long startSecs = System.currentTimeMillis() / 1000;

        JSONObject wrapperJson = JSONObject.parseObject(reqString);
        JSONObject payloadObj = wrapperJson.getJSONObject("payload");
        String version = payloadObj.getString(FullPullConstants.REQ_PAYLOAD_VERSION);
        String batchNo = payloadObj.getString(FullPullConstants.REQ_PAYLOAD_BATCH_NO);

        DBConfiguration dbConf = FullPullHelper.getDbConfiguration(reqString);
        FullPullHelper.createProgressZkNode(zkService, dbConf);

        // 更新zk monitor节点信息
        ProgressInfoParam infoParam = new ProgressInfoParam();
        infoParam.setCreateTime(currentTimeStampString);
        infoParam.setStartTime(currentTimeStampString);
        infoParam.setStartSecs(startSecs);
        infoParam.setStatus(FullPullConstants.FULL_PULL_STATUS_SPLITTING);
        infoParam.setVersion(version);
        infoParam.setBatchNo(batchNo);
        ProgressInfo progressInfo = FullPullHelper.updateZkNodeInfoWithVersion(zkService, FullPullHelper.getMonitorNodePath(reqString), infoParam);
        FullPullHelper.updateStatusToFullPullHistoryTable(progressInfo, FullPullHelper.getSeqNo(reqJson), null, null, null);
    }

    private boolean canPrintNow() {
        suppressLoggingCount++;
        if (suppressLoggingCount % EMPTY_RUN_COUNT == 0) {
            suppressLoggingCount = 0;
            return true;
        }
        return false;
    }

    private void loadRunningConf(String reloadMsgJson) {
        String notifyEvtName = reloadMsgJson == null ? "loaded" : "reloaded";
        try {
            this.confMap = FullPullHelper.loadConfProps(zkConnect, topologyId, dsName, zkTopoRoot, "SplitSpout");
            this.commonProps = (Properties) confMap.get(FullPullHelper.RUNNING_CONF_KEY_COMMON);
            this.consumer = (Consumer<String, byte[]>) confMap.get(FullPullHelper.RUNNING_CONF_KEY_CONSUMER);
            this.zkService = (ZkService) confMap.get(FullPullHelper.RUNNING_CONF_KEY_ZK_SERVICE);
            logger.info("[split spout] Running Config is " + notifyEvtName + " successfully for DataShardsSplittingSpout!");
        } catch (Exception e) {
            logger.error(notifyEvtName + "ing running configuration encountered Exception!");
            throw e;
        } finally {
            FullPullHelper.saveReloadStatus(reloadMsgJson, "splitting-spout", true, zkConnect);
        }
    }

    private void destory() {
        if (consumer != null) {
            consumer.close();
            consumer = null;
        }
        logger.info("close consumer");
    }

}
