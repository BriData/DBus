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
import com.creditease.dbus.common.bean.FullPullHistory;
import com.creditease.dbus.common.bean.ProgressInfo;
import com.creditease.dbus.common.bean.ProgressInfoParam;
import com.creditease.dbus.commons.ZkService;
import com.creditease.dbus.commons.exception.InitializationException;
import com.creditease.dbus.helper.FullPullHelper;
import com.creditease.dbus.spout.queue.ShardElement;
import com.creditease.dbus.spout.queue.ShardsProcessManager;
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

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

/**
 * 读取kafka数据的Spout实现
 * Created by Shrimp on 16/6/2.
 */
public class DataPullingSpout extends BaseRichSpout {
    private static final long serialVersionUID = 8844465866790614624L;
    private Logger logger = LoggerFactory.getLogger(getClass());
    private final String zkTopoRoot = FullPullConstants.TOPOLOGY_ROOT + "/" + FullPullConstants.FULL_PULLING_PROPS_ROOT;

    private SpoutOutputCollector collector;
    private String zkConnect;
    private String topologyId;
    private String dsName;
    private Map confMap;
    final private int EMPTY_RUN_COUNT = 60000;
    private int suppressLoggingCount = EMPTY_RUN_COUNT / 2;
    private Properties commonProps;

    private ZkService zkService = null;
    private Consumer<String, byte[]> consumer;

    //曾经出现过错误的split任务
    private HashSet<String> failAndBreakTuplesSet = new HashSet<>();
    //进行中的分片任务
    private ShardsProcessManager shardsProcessManager = null;

    /**
     * 初始化collectors
     */
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        this.collector = collector;
        this.zkConnect = (String) conf.get(FullPullConstants.ZKCONNECT);
        this.topologyId = (String) conf.get(FullPullConstants.FULL_PULLER_TOPOLOGY_ID);
        this.dsName = (String) conf.get(FullPullConstants.DS_NAME);
        this.shardsProcessManager = new ShardsProcessManager();
        try {
            //设置topo类型，用于获取配置信息路径
            FullPullHelper.setTopologyType(FullPullConstants.FULL_PULLER_TYPE);
            loadRunningConf(null);
            FullPullHelper.updatePendingTasksToHistoryTable(dsName, FullPullConstants.FULLPULL_PENDING_TASKS_OP_PULL_TOPOLOGY_RESTART, consumer,
                    commonProps.getProperty(FullPullConstants.FULL_PULL_MEDIANT_TOPIC));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new InitializationException();
        }
        logger.info("[pull spout] {} init complete!", topologyId);
    }

    /**
     * 在SpoutTracker类中被调用，每调用一次就可以向storm集群中发射一条数据（一个tuple元组），该方法会被不停的调用
     */
    @Override
    public void nextTuple() {
        try {
            // 读取kafka消息
            ConsumerRecords<String, byte[]> records = consumer.poll(0);
            // 判断是否有数据被读取
            if (records.isEmpty()) {
                if (canPrintNow()) {
                    logger.info("[pull spout] running ...");
                }
                return;
            }

            // 按记录进行处理
            for (ConsumerRecord<String, byte[]> record : records) {
                String key = record.key();
                if (null == key) {
                    logger.error("the key of splitting record {} is null on DataPullingSpout!", record);
                    continue;
                }

                String wrapperString = new String(record.value());
                JSONObject wrapperJson = JSONObject.parseObject(wrapperString);
                String reqString = wrapperJson.getString(FullPullConstants.FULLPULL_REQ_PARAM);
                try {
                    if (key.equals(FullPullConstants.COMMAND_FULL_PULL_RELOAD_CONF) || key.equals(FullPullConstants.COMMAND_FULL_PULL_FINISH_REQ)) {
                        logger.info("[pull spout] receive config reloading request offset:{},key:{},value:{}", record.offset(), record.key(), reqString);
                        destory();
                        loadRunningConf(reqString);
                        collector.emit(FullPullConstants.CTRL_STREAM, new Values(wrapperString));
                        logger.info("[pull spout] config for pull spout is reloaded successfully!");
                    } else if (key.equals(FullPullConstants.DATA_EVENT_INDEPENDENT_FULL_PULL_REQ)) {
                        logger.info("[pull spout] 收到全量拉取任务:topic:{}, offset:{}, key:{}", record.topic(), record.offset(), record.key());
                        if (!failAndBreakTuplesSet.contains(reqString)) {
                            JSONObject reqJson = JSONObject.parseObject(reqString);
                            Long id = FullPullHelper.getSeqNo(reqJson);
                            Long splitIndex = wrapperJson.getLong(FullPullConstants.DATA_CHUNK_SPLIT_INDEX);
                            //处理任务列表
                            shardsProcessManager.addMessage(id, record.offset(), splitIndex);
                            String dsKey = FullPullHelper.getDataSourceKey(JSONObject.parseObject(reqString));
                            if (splitIndex.equals("1")) {
                                startPullingReport(zkService, reqString);
                            }
                            collector.emit(new Values(wrapperString), record);
                            logger.info("[pull emit] dsKey:{}, topic:{}, offset: {}, split index:{}, key: {}", dsKey, record.topic(), record.offset(), splitIndex, record.key());
                        } else {
                            logger.info("[pull spout] skipped offset(have received fail ack)--------is:{}", record.offset());
                        }
                    }
                } catch (Exception e) {
                    String errorMsg = "[pull spout] spout-->bolt exception!" + e.getMessage();
                    logger.error(errorMsg, e);
                    if (key.equals(FullPullConstants.DATA_EVENT_INDEPENDENT_FULL_PULL_REQ)) {
                        FullPullHelper.finishPullReport(reqString, FullPullConstants.FULL_PULL_STATUS_ABORT, errorMsg);
                    }
                }
            }
            consumer.commitSync();
        } catch (Exception e) {
            logger.error("Exception happended on pull spout nextTuple()", e);
        }

    }

    /**
     * 定义字段id，该id在简单模式下没有用处，但在按照字段分组的模式下有很大的用处。
     * 该declarer变量有很大作用，我们还可以调用declarer.declareStream();来定义stramId，该id可以用来定义更加复杂的流拓扑结构
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("source"));
        declarer.declareStream(FullPullConstants.CTRL_STREAM, new Fields("source"));
    }

    @Override
    public void ack(Object msgId) {
        try {
            if (msgId != null && ConsumerRecord.class.isInstance(msgId)) {
                ConsumerRecord<String, byte[]> record = getMessageId(msgId);
                String wrapperString = new String(record.value());
                JSONObject wrapperJson = JSONObject.parseObject(wrapperString);
                String reqString = wrapperJson.getString(FullPullConstants.FULLPULL_REQ_PARAM);
                Long id = FullPullHelper.getSeqNo(reqString);
                Long splitIndex = wrapperJson.getLong(FullPullConstants.DATA_CHUNK_SPLIT_INDEX);

                ShardElement shardElement = shardsProcessManager.okAndGetCommitPoint(id, record.offset());
                if (shardElement != null) {
                    //更新拉取offset到历史表
                    FullPullHistory fullPullHistory = new FullPullHistory();
                    fullPullHistory.setId(id);
                    fullPullHistory.setCurrentShardOffset(shardElement.getOffset());
                    FullPullHelper.updateStatusToFullPullHistoryTable(fullPullHistory);
                    shardsProcessManager.committed(id);
                }
                logger.info("[pull ack] topic: {}, offset: {}, split index:{},  key: {}", record.topic(), record.offset(), splitIndex, record.key());
            }
            super.ack(msgId);
        } catch (Exception e) {
            logger.error("[pull ack] exception!", e);
        }
    }

    @Override
    public void fail(Object msgId) {
        try {
            if (msgId != null && ConsumerRecord.class.isInstance(msgId)) {
                ConsumerRecord<String, byte[]> record = getMessageId(msgId);
                JSONObject wrapperJson = JSONObject.parseObject(new String(record.value()));
                String reqString = wrapperJson.getString(FullPullConstants.FULLPULL_REQ_PARAM);
                Long id = FullPullHelper.getSeqNo(reqString);
                String splitIndex = wrapperJson.getString(FullPullConstants.DATA_CHUNK_SPLIT_INDEX);
                logger.error("[pull fail] topic: {}, offset: {}, split index:{},  key: {}", record.topic(), record.offset(), splitIndex, record.key());

                shardsProcessManager.failAndClearShardElementQueue(id, record.offset());

                //写monitor，并且发送错误返回等， 只报错一次
                if (!failAndBreakTuplesSet.contains(reqString)) {
                    FullPullHelper.finishPullReport(reqString, FullPullConstants.FULL_PULL_STATUS_ABORT, null);
                }
                sendFinishMsgToBolt(JSONObject.parseObject(reqString));
                failAndBreakTuplesSet.add(reqString);
            }
            super.fail(msgId);
        } catch (Exception e) {
            logger.error("[pull fail] exception!", e);
        }
    }

    private void sendFinishMsgToBolt(JSONObject reqJson) throws Exception {
        reqJson.put("type", FullPullConstants.COMMAND_FULL_PULL_FINISH_REQ);
        JSONObject wrapperJson = new JSONObject();
        wrapperJson.put(FullPullConstants.FULLPULL_REQ_PARAM, reqJson.toJSONString());
        collector.emit(FullPullConstants.CTRL_STREAM, new Values(wrapperJson.toJSONString()));
        logger.info("[emit] full pull finish msg to pull bolt ,{}", wrapperJson);
    }

    private <T> T getMessageId(Object msgId) {
        return (T) msgId;
    }

    private void startPullingReport(ZkService zkService, String reqString) throws Exception {
        ProgressInfoParam infoParam = new ProgressInfoParam();
        infoParam.setStatus(FullPullConstants.FULL_PULL_STATUS_PULLING);
        ProgressInfo progressInfo = FullPullHelper.updateZkNodeInfoWithVersion(zkService, FullPullHelper.getMonitorNodePath(reqString), infoParam);
        FullPullHelper.updateStatusToFullPullHistoryTable(progressInfo, FullPullHelper.getSeqNo(reqString), TimeUtils.getCurrentTimeStampString(), null, null);
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
            this.confMap = FullPullHelper.loadConfProps(zkConnect, topologyId, dsName, zkTopoRoot, "PullSpout");
            this.commonProps = (Properties) confMap.get(FullPullHelper.RUNNING_CONF_KEY_COMMON);
            this.zkService = (ZkService) confMap.get(FullPullHelper.RUNNING_CONF_KEY_ZK_SERVICE);
            this.consumer = (Consumer<String, byte[]>) confMap.get(FullPullHelper.RUNNING_CONF_KEY_CONSUMER);
            logger.info("[pull spout] Running Config is " + notifyEvtName + " successfully for DataPullingSpout!");
        } catch (Exception e) {
            logger.error("Loading running configuration encountered Exception!");
            throw e;
        } finally {
            FullPullHelper.saveReloadStatus(reloadMsgJson, "pulling-spout", true, zkConnect);
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
