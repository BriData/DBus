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

package com.creditease.dbus.spout;

import java.util.*;
import java.util.concurrent.TimeUnit;

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

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.common.CommandCtrl;
import com.creditease.dbus.common.DataPullConstants;
import com.creditease.dbus.common.FullPullHelper;
import com.creditease.dbus.common.ProgressInfo;
import com.creditease.dbus.common.TopoKillingStatus;
import com.creditease.dbus.common.utils.JsonUtil;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.Constants.ZkTopoConfForFullPull;
import com.creditease.dbus.commons.ZkService;
import com.creditease.dbus.commons.exception.InitializationException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * 读取kafka数据的Spout实现
 * Created by Shrimp on 16/6/2.
 */
public class DataPullingSpout extends BaseRichSpout {
    private Logger LOG = LoggerFactory.getLogger(getClass());

    private final String zkTopoRoot = Constants.TOPOLOGY_ROOT + "/" + Constants.FULL_PULLING_PROPS_ROOT;

    //用来发射数据的工具类
    private SpoutOutputCollector collector;
    private String zkConnect;
    private String topologyId;
    private boolean isGlobal;       //是否是独立拉全量
    private String zkMonitorRootNodePath;
    private String dsName;

    // 限流控制, 这个与consumer参数类似, 可能可以不需要了
    private int MAX_FLOW_THRESHOLD;
    //只关心流入数据的情况
    private int flowedMsgCount = 0;

    private Map confMap;
    private Properties commonProps;
    private ZkService zkService = null;
    private Consumer<String, byte[]> consumer;

    //pending的任务池
    private Set pendingTasksSet = new HashSet();

    //曾经出现过错误的split任务
    private HashSet<String> failAndBreakTuplesSet = new HashSet<>();

    //variable about stop
    private int stopFlag = TopoKillingStatus.RUNNING.status;
    private long startTime = 0;
    private int processedCount = 0;   //收到ok或fail的ack数
    private int emittedCount = 0;     //只记录拉取消息的emit出去的条数，reload等控制信息不记录数
    //经过研究processedCount == emittedCount时，就是 flowedMsgCount = 0的时候，也就说 processedCount和emittedCount 没有存在的意义

    final private int EMPTY_RUN_COUNT = 60000;
    private int suppressLoggingCount = EMPTY_RUN_COUNT / 2;

    /**
     * 初始化collectors
     */
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        LOG.info("Pulling Spout {} is starting!", topologyId);
        this.collector = collector;
        this.zkConnect = (String) conf.get(Constants.StormConfigKey.ZKCONNECT);
        this.topologyId = (String) conf.get(Constants.StormConfigKey.FULL_PULLER_TOPOLOGY_ID);

        this.isGlobal = this.topologyId.toLowerCase().
                indexOf(ZkTopoConfForFullPull.GLOBAL_FULLPULLER_TOPO_PREFIX) != -1 ? true : false;
        if (this.isGlobal) {
            this.zkMonitorRootNodePath = Constants.FULL_PULL_MONITOR_ROOT_GLOBAL;
        } else {
            this.zkMonitorRootNodePath = Constants.FULL_PULL_MONITOR_ROOT;
        }

        try {
            //设置topo类型，用于获取配置信息路径
            FullPullHelper.setTopologyType(Constants.FULL_PULLER_TYPE);

            loadRunningConf(null);
            // 检查是否有遗留未决的拉取任务。如果有，resolve（发resume消息通知给appender，并在zk上记录，且将监测到的pending任务写日志，方便排查问题）。
            // 对于已经resolve的pending任务，将其移出pending队列，以免造成无限重复处理。
            FullPullHelper.updatePendingTasksTrackInfo(zkService, dsName, null, DataPullConstants.FULLPULL_PENDING_TASKS_OP_CRASHED_NOTIFY);
            FullPullHelper.updatePendingTasksToHistoryTable(dsName, DataPullConstants.FULLPULL_PENDING_TASKS_OP_PULL_TOPOLOGY_RESTART, consumer,
                    commonProps.getProperty(Constants.ZkTopoConfForFullPull.FULL_PULL_MEDIANT_TOPIC));
        } catch (Exception e) {
            LOG.error(e.getMessage(),e);
            throw new InitializationException();
        }
        LOG.info("Pulling Spout {} is started!", topologyId);
    }

    /**
     * delay Print message
     *
     * @return
     */
    private boolean canPrintNow() {
        suppressLoggingCount++;
        if (suppressLoggingCount % EMPTY_RUN_COUNT == 0) {
            suppressLoggingCount = 0;
            return true;
        }
        return false;
    }

    /**
     * 在SpoutTracker类中被调用，每调用一次就可以向storm集群中发射一条数据（一个tuple元组），该方法会被不停的调用
     */
    @Override
    public void nextTuple() {
        // 限流
        if (flowedMsgCount >= MAX_FLOW_THRESHOLD) {
            LOG.info("Flow control: Spout gets {} pieces of msg.", flowedMsgCount);
            try {
                TimeUnit.MILLISECONDS.sleep(10);
            } catch (InterruptedException e) {
                LOG.error(e.getMessage(), e);
            }
            return;
        }

        if (stopFlag > TopoKillingStatus.RUNNING.status) {
            if (stopFlag == TopoKillingStatus.STOPPING.status) {
                try {
                    long timeout = System.currentTimeMillis() - startTime;
                    String toposKillWaitTimeout = commonProps.getProperty(Constants.ZkTopoConfForFullPull.TOPOS_KILL_WAIT_TIMEOUT);
                    long toposKillWaitTimeConf = toposKillWaitTimeout == null
                            ? Constants.ZkTopoConfForFullPull.TOPOS_KILL_WAIT_TIMEOUT_DEFAULT_VAL
                            : Long.valueOf(toposKillWaitTimeout);
                    if (emittedCount == processedCount || timeout / 1000 >= toposKillWaitTimeConf) {
                        ObjectMapper mapper = new ObjectMapper();
                        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

                        // Call storm api to kill topo
                        int topoKillWaitTime = Constants.ZkTopoConfForFullPull.TOPOS_KILL_STORM_API_WAITTIME_PARAM_DEFAULT_VAL;
                        String topoKillWaitTimeParam = commonProps.getProperty(Constants.ZkTopoConfForFullPull.TOPOS_KILL_STORM_API_WAITTIME_PARAM);
                        try {
                            topoKillWaitTime = Integer.valueOf(topoKillWaitTimeParam);
                        } catch (Exception e) {
                            // Just ignore, use the default value
                        }
                        String killResult = CommandCtrl.killTopology(zkService, topologyId, topoKillWaitTime);
                        stopFlag = TopoKillingStatus.READY_FOR_KILL.status;
                        consumer.commitSync();
                        LOG.info("Id为 {}的Topology Kill已結束.Kill结果：{}.", topologyId, killResult);
                    }
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    String errorMsg = "Encountered exception when writing msg to zk monitor.";
                    LOG.error(errorMsg, e);
                }
            }
            return;
        }

        try {
            // 读取kafka消息
            ConsumerRecords<String, byte[]> records = consumer.poll(0);
            // 判断是否有数据被读取
            if (records.isEmpty()) {
                if (canPrintNow()) {
                    LOG.info("Pulling Spout running ...");
                }
                return;
            }

            // 按记录进行处理
            for (ConsumerRecord<String, byte[]> record : records) {
                String key = record.key();
                if (null == key) {
                    LOG.error("the key of splitting record {} is null on DataPullingSpout!", record);
                    continue;
                }

                String msg = new String(record.value());
                //TODODO 对于COMMAND_FULL_PULL_STOP 以下语句是否会出错？
                JSONObject jsonObject = JSONObject.parseObject(msg);
                String dataSourceInfo = jsonObject.getString(DataPullConstants.DATA_SOURCE_INFO);

                try {
                    if ((key.equals(DataPullConstants.COMMAND_FULL_PULL_RELOAD_CONF))) {
                        LOG.info("Spout receive reload event, Record offset--------is:{}", record.offset());
                        loadRunningConf(dataSourceInfo);

                        // 传导给bolt。不跟踪消息的处理状态，即不会调用ack或者fail
                        collector.emit(new Values(msg));

                    } else if ((key.equals(DataPullConstants.COMMAND_FULL_PULL_STOP))) {
                        LOG.info("Spout receive stop event, Record offset--------is:{}", record.offset());
                        stopFlag = TopoKillingStatus.STOPPING.status;
                        startTime = System.currentTimeMillis();

                    } else if ((key.equals(DataPullConstants.DATA_EVENT_FULL_PULL_REQ))
                            || (key.equals(DataPullConstants.DATA_EVENT_INDEPENDENT_FULL_PULL_REQ))) {
                        // 对于每次拉取，只在第一次将当前任务添加到pending tasks list.以避免拉取轮数多时，频繁访问zk。
                        if (pendingTasksSet.add(dataSourceInfo)) {
                            FullPullHelper.updatePendingTasksTrackInfo(zkService, dsName, dataSourceInfo, DataPullConstants.FULLPULL_PENDING_TASKS_OP_ADD_WATCHING);
                        }

                        if (!failAndBreakTuplesSet.contains(dataSourceInfo)) {
                            emittedCount++;
                            String dsKey = FullPullHelper.getDataSourceKey(JSONObject.parseObject(dataSourceInfo));
                            String splitIndex = jsonObject.getString(DataPullConstants.DATA_CHUNK_SPLIT_INDEX);
                            LOG.info("Spout read Record offset--------is:{}, {} spout-->bolt the split index is {}", record.offset(), dsKey, splitIndex);
                            if (splitIndex.equals("1")) {
                                startPullingReport(zkService, dataSourceInfo);
                            }
                            flowedMsgCount++;
                            collector.emit(new Values(msg), record);
                        } else {
                            LOG.info("Spout skipped Record offset(have received fail ack)--------is:{}", record.offset());
                        }
                    }
                } catch (Exception e) {
                    String errorMsg = "DataPullingSpout:spout-->bolt exception!" + e.getMessage();
                    LOG.error(e.getMessage(),e);
                    LOG.error(errorMsg, e);
                    //处理悬而未决的任务和发送拉取报告
                    if (key.equals(DataPullConstants.DATA_EVENT_FULL_PULL_REQ)
                            || key.equals(DataPullConstants.DATA_EVENT_FULL_PULL_REQ)) {
                        FullPullHelper.updatePendingTasksTrackInfo(zkService, dsName, dataSourceInfo, DataPullConstants.FULLPULL_PENDING_TASKS_OP_REMOVE_WATCHING);
                        FullPullHelper.finishPullReport(zkService, dataSourceInfo,
                                FullPullHelper.getCurrentTimeStampString(), Constants.DataTableStatus.DATA_STATUS_ABORT, errorMsg);
                    }
                }
            }
            consumer.commitSync();
        } catch (Exception e) {
            LOG.error("DataPullingSpout:spout-->bolt exception!", e);
        }

    }

    /**
     * 定义字段id，该id在简单模式下没有用处，但在按照字段分组的模式下有很大的用处。
     * 该declarer变量有很大作用，我们还可以调用declarer.declareStream();来定义stramId，该id可以用来定义更加复杂的流拓扑结构
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("source")); //collector.emit(new Values(msg));参数要对应
    }

    @Override
    public void ack(Object msgId) {
        try {
            if (msgId != null && ConsumerRecord.class.isInstance(msgId)) {
                flowedMsgCount--;

                ConsumerRecord<String, byte[]> record = getMessageId(msgId);
                String recordString = new String(record.value());
                JSONObject jsonObject = JSONObject.parseObject(recordString);
                String dataSourceInfo = jsonObject.getString(DataPullConstants.DATA_SOURCE_INFO);
                String dsKey = FullPullHelper.getDataSourceKey(JSONObject.parseObject(dataSourceInfo));
                String splitIndex = jsonObject.getString(DataPullConstants.DATA_CHUNK_SPLIT_INDEX);
                LOG.info("Acked Record offset--------is:{}, {}:split index is {}", record.offset(), dsKey, splitIndex);
            }

            processedCount++;
            super.ack(msgId);
        } catch (Exception e) {
            LOG.error("DataPullingSpout:ack throwed exception!", e);
        }
    }

    @Override
    public void fail(Object msgId) {
        try {
            if (msgId != null && ConsumerRecord.class.isInstance(msgId)) {
                flowedMsgCount--;

                ConsumerRecord<String, byte[]> record = getMessageId(msgId);
                String recordString = new String(record.value());
                JSONObject jsonObject = JSONObject.parseObject(recordString);
                String dataSourceInfo = jsonObject.getString(DataPullConstants.DATA_SOURCE_INFO);
                String dsKey = FullPullHelper.getDataSourceKey(JSONObject.parseObject(dataSourceInfo));
                String splitIndex = jsonObject.getString(DataPullConstants.DATA_CHUNK_SPLIT_INDEX);
                LOG.error("Spout got fail!, record offset is:{}, {}: split index is {}", record.offset(), dsKey, splitIndex);

                //写monitor，并且发送错误返回等， 只报错一次
                if (!failAndBreakTuplesSet.contains(dataSourceInfo)) {
                    String errMsg = String.format("Spout got fail!, record offset is:%s, %s: split index is %s", record.offset(), dsKey, splitIndex);
                    FullPullHelper.finishPullReport(zkService, dataSourceInfo, FullPullHelper.getCurrentTimeStampString(),
                            Constants.DataTableStatus.DATA_STATUS_ABORT, errMsg);
                    FullPullHelper.updatePendingTasksTrackInfo(zkService, dsName, dataSourceInfo, DataPullConstants.FULLPULL_PENDING_TASKS_OP_REMOVE_WATCHING);
                }
                failAndBreakTuplesSet.add(dataSourceInfo);
            }

            processedCount++;
            super.fail(msgId);
        } catch (Exception e) {
            LOG.error("DataPullingSpout:Fail ack throwed exception!", e);
        }
    }

    private <T> T getMessageId(Object msgId) {
        return (T) msgId;
    }


    /**
     * 更新zk上monitor节点信息为pulling状态
     *
     * @param zkService
     * @param dataSourceInfo
     */
    private void startPullingReport(ZkService zkService, String dataSourceInfo) {
        String currentTimeStampString = FullPullHelper.getCurrentTimeStampString();
        String progressInfoNodePath = FullPullHelper.getMonitorNodePath(dataSourceInfo);
        try {
            //只是更新monitor节点状态
            ProgressInfo progressObj = FullPullHelper.getMonitorInfoFromZk(zkService, progressInfoNodePath);
            progressObj.setUpdateTime(currentTimeStampString);
            progressObj.setStatus(Constants.FULL_PULL_STATUS_PULLING);

            ObjectMapper mapper = JsonUtil.getObjectMapper();
            FullPullHelper.updateZkNodeInfo(zkService, progressInfoNodePath, mapper.writeValueAsString(progressObj));

            //开始拉取写拉取状态
            JSONObject fullpullUpdateParams = new JSONObject();
            fullpullUpdateParams.put("dataSourceInfo", dataSourceInfo);
            fullpullUpdateParams.put("status", "pulling");
            fullpullUpdateParams.put("start_pull_time", currentTimeStampString);
            FullPullHelper.writeStatusToDbManager(fullpullUpdateParams);
        } catch (Exception e) {
            String errorMsg = "Encountered exception when writing msg to zk monitor.";
            LOG.error(errorMsg, e);
        }
    }

    private void loadRunningConf(String reloadMsgJson) {
        try {
            this.confMap = FullPullHelper.loadConfProps(zkConnect, topologyId, zkTopoRoot, Constants.ZkTopoConfForFullPull.FULL_PULL_MEDIANT_TOPIC);
            this.MAX_FLOW_THRESHOLD = (Integer) confMap.get(FullPullHelper.RUNNING_CONF_KEY_MAX_FLOW_THRESHOLD);
            this.commonProps = (Properties) confMap.get(FullPullHelper.RUNNING_CONF_KEY_COMMON);
            this.dsName = commonProps.getProperty(Constants.ZkTopoConfForFullPull.DATASOURCE_NAME);
            this.zkService = (ZkService) confMap.get(FullPullHelper.RUNNING_CONF_KEY_ZK_SERVICE);
            this.consumer = (Consumer<String, byte[]>) confMap.get(FullPullHelper.RUNNING_CONF_KEY_CONSUMER);


            String notifyEvtName = reloadMsgJson == null ? "loaded" : "reloaded";
            LOG.info("Running Config is " + notifyEvtName + " successfully for DataPullingSpout!");
        } catch (Exception e) {
            LOG.error("Loading running configuration encountered Exception!", e);
            throw e;
        } finally {
            FullPullHelper.saveReloadStatus(reloadMsgJson, "pulling-spout", true, zkConnect);
        }
    }
}
