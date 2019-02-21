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

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.common.DBHelper;
import com.creditease.dbus.common.DataPullConstants;
import com.creditease.dbus.common.FullPullHelper;
import com.creditease.dbus.common.TopoKillingStatus;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.Constants.ZkTopoConfForFullPull;
import com.creditease.dbus.commons.ZkService;
import com.creditease.dbus.commons.exception.InitializationException;
import org.apache.commons.lang.StringUtils;
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
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * 读取kafka数据的Spout实现 Created by Shrimp on 16/6/2.
 */
public class DataShardsSplittingSpout extends BaseRichSpout {
    private Logger LOG = LoggerFactory.getLogger(getClass());
    private static final long serialVersionUID = 1L;

    private long overflowCount = 0;

    // 用来发射数据的工具类
    private SpoutOutputCollector collector;

    private String zkConnect;
    private String topologyId;
    //是否是独立拉全量
    private boolean isGlobal;
    private String zkMonitorRootNodePath;
    private String dsName;

    private final String zkTopoRoot = Constants.TOPOLOGY_ROOT + "/" + Constants.FULL_SPLITTING_PROPS_ROOT;
    private String fullPullSrcTopic = "";

    private int flowedMsgCount = 0;
    private int emittedCount = 0;
    private int processedCount = 0;

    //停服处理
    private int stopFlag = TopoKillingStatus.RUNNING.status;
    private String stopRecord = null;

    private long startTime = 0;

    private Map confMap;

    private Consumer<String, byte[]> consumer;
    ZkService zkService = null;
    private int MAX_FLOW_THRESHOLD;
    Properties commonProps;

    final private int EMPTY_RUN_COUNT = 60000;
    private int suppressLoggingCount = EMPTY_RUN_COUNT / 2;

    /**
     * 初始化collector
     */
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        LOG.info("Splitting Spout {} is starting!", topologyId);
        this.collector = collector;
        this.zkConnect = (String) conf.get(Constants.StormConfigKey.ZKCONNECT);
        this.topologyId = (String) conf.get(Constants.StormConfigKey.FULL_SPLITTER_TOPOLOGY_ID);
        this.isGlobal = this.topologyId.toLowerCase().indexOf(ZkTopoConfForFullPull.GLOBAL_FULLPULLER_TOPO_PREFIX) != -1 ? true : false;
        if (this.isGlobal) {
            this.zkMonitorRootNodePath = Constants.FULL_PULL_MONITOR_ROOT_GLOBAL;
        } else {
            this.zkMonitorRootNodePath = Constants.FULL_PULL_MONITOR_ROOT;
        }
        LOG.info("topologyId:{} zkConnect:{} zkTopoRoot:{}", topologyId, zkConnect, zkTopoRoot);
        try {
            //设置topo类型，用于获取配置信息路径
            FullPullHelper.setTopologyType(Constants.FULL_SPLITTER_TYPE);

            loadRunningConf(null);

            // 检查是否有遗留未决的拉取任务。如果有，resolve（发resume消息通知给appender，并在zk上记录，且将监测到的pending任务写日志，方便排查问题）。
            // 对于已经resolve的pending任务，将其移出pending队列，以免造成无限重复处理。
            FullPullHelper.updatePendingTasksTrackInfo(zkService, dsName, null, DataPullConstants.FULLPULL_PENDING_TASKS_OP_CRASHED_NOTIFY);

            FullPullHelper.updatePendingTasksToHistoryTable(dsName, DataPullConstants.FULLPULL_PENDING_TASKS_OP_SPLIT_TOPOLOGY_RESTART,
                    consumer, commonProps.getProperty(Constants.ZkTopoConfForFullPull.FULL_PULL_SRC_TOPIC));

        } catch (Exception e) {
            LOG.error(e.getMessage(),e);
            throw new InitializationException();
        }
        LOG.info("Splitting Spout {} is started!", topologyId);
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
            try {
                TimeUnit.MILLISECONDS.sleep(100);
                if (overflowCount % 100 == 0) {
                    LOG.warn("splitSPout overflow, flowedMsgCount: {}, MAX_FLOW_THRESHOLD: {}.", flowedMsgCount, MAX_FLOW_THRESHOLD);
                }
                overflowCount++;

            } catch (InterruptedException e) {
                LOG.error(e.getMessage(), e);
            }
            return;
        }
        overflowCount = 0;


        //通过命令删除topology的功能，可能可以移除
        if (stopFlag > TopoKillingStatus.RUNNING.status) {
            if (stopFlag == TopoKillingStatus.STOPPING.status) {
                try {
                    long timeout = System.currentTimeMillis() - startTime;
                    String toposKillWaitTimeout = commonProps
                            .getProperty(Constants.ZkTopoConfForFullPull.TOPOS_KILL_WAIT_TIMEOUT);
                    long toposKillWaitTimeConf = toposKillWaitTimeout == null
                            ? Constants.ZkTopoConfForFullPull.TOPOS_KILL_WAIT_TIMEOUT_DEFAULT_VAL
                            : Long.valueOf(toposKillWaitTimeout);
                    if (emittedCount == processedCount || timeout / 1000 >= toposKillWaitTimeConf) {
                        // 获取Retry等待时间配置
                        String toposKillWaitTimeForRetries = commonProps
                                .getProperty(Constants.ZkTopoConfForFullPull.TOPOS_KILL_WAIT_TIME_FOR_RETRIES);
                        long toposKillWaitTimeForRetriesConf = toposKillWaitTimeForRetries == null
                                ? Constants.ZkTopoConfForFullPull.TOPOS_KILL_WAIT_TIME_FOR_RETRIES_DEFAULT_VAL
                                : Long.valueOf(toposKillWaitTimeForRetries);
                        // 适当延迟一会发送stop通知msg，以避免retry的情况下，stop指令先到达
                        Thread.sleep(toposKillWaitTimeForRetriesConf * 1000);

                        collector.emit(new Values(stopRecord), stopRecord);
                        stopFlag = TopoKillingStatus.READY_FOR_KILL.status;

                        consumer.commitSync();
                    }
                } catch (Exception e) {
                    LOG.error("DataShardsSplittingSpout处理停服命令异常！", e);
                }
            }
            return;
        }

        try {
            ConsumerRecords<String, byte[]> records = consumer.poll(0);
            // 判断是否有数据被读取
            if (records.isEmpty()) {
                if (canPrintNow()) {
                    LOG.info("Splitting Spout running ...");
                }
                return;
            }
            // 按记录进行处理
            for (ConsumerRecord<String, byte[]> record : records) {
                String key = record.key();
                if (null == key) {
                    continue;
                }
                String dataSourceInfo = new String(record.value());
                try {
                    if (key.equals(DataPullConstants.COMMAND_FULL_PULL_RELOAD_CONF)) {
                        loadRunningConf(dataSourceInfo);
                        LOG.info("Received conf reloading request: {}", dataSourceInfo);
                        collector.emit(new Values(dataSourceInfo));
                        LOG.info("Conf for SplittingSpout is reloaded successfully!");

                    } else if (key.equals(DataPullConstants.COMMAND_FULL_PULL_STOP)) {
                        stopFlag = TopoKillingStatus.STOPPING.status;
                        startTime = System.currentTimeMillis();
                        // 不跟踪消息的处理状态，即不会调用ack或者fail
                        collector.emit(new Values(dataSourceInfo));
                    } else if (key.equals(DataPullConstants.DATA_EVENT_FULL_PULL_REQ)
                            || key.equals(DataPullConstants.DATA_EVENT_INDEPENDENT_FULL_PULL_REQ)) {
                        LOG.info("Received full pull request: {}", dataSourceInfo);

                        //判断任务是否安全
                        if (!confirmTaskIsSecurity(dataSourceInfo)) {
                            return;
                        }
                        //判断能否继续分片(上一个任务状态必须是Ending/abort状态才可以开始新的分片)
                        waitForLastTaskEnd(dataSourceInfo);
                        //处理消息
                        flowedMsgCount++;

                        // 每次拉取都要进行批次号加1处理。这条语句的位置不要变动。
                        dataSourceInfo = increseBatchNo(dataSourceInfo);
                        FullPullHelper.updatePendingTasksTrackInfo(zkService, dsName, dataSourceInfo, DataPullConstants.FULLPULL_PENDING_TASKS_OP_ADD_WATCHING);
                        //创建zk节点并在zk节点上输出拉取信息
                        FullPullHelper.startSplitReport(zkService, dataSourceInfo);
                        collector.emit(new Values(dataSourceInfo), record);
                        emittedCount++;
                    } else {
                        LOG.info("Ignore other command: {}", key);
                    }

                } catch (Exception e) {
                    String errorMsg = "Exception happened when processing topic on " + fullPullSrcTopic
                            + ". Exception Info:" + e.getMessage();
                    LOG.error(errorMsg, e);

                    if (key.equals(DataPullConstants.DATA_EVENT_FULL_PULL_REQ)
                            || key.equals(DataPullConstants.DATA_EVENT_FULL_PULL_REQ)) {
                        FullPullHelper.updatePendingTasksTrackInfo(zkService, dsName, dataSourceInfo, DataPullConstants.FULLPULL_PENDING_TASKS_OP_REMOVE_WATCHING);
                        FullPullHelper.finishPullReport(zkService, dataSourceInfo, FullPullHelper.getCurrentTimeStampString(),
                                Constants.DataTableStatus.DATA_STATUS_ABORT, errorMsg);
                    }
                    throw e;
                }
            }
        } catch (Exception e) {
            LOG.error("Splitting encountered exception.", e);
            throw e;
        }
    }

    /**
     * 检查收到的任务是否是安全的任务
     *
     * @param dataSourceInfo
     */
    private boolean confirmTaskIsSecurity(String dataSourceInfo) {
        JSONObject jsonObject = JSONObject.parseObject(dataSourceInfo);
        JSONObject project = jsonObject.getJSONObject("project");
        //非多租户暂时无法控制安全问题
        if (project == null || project.isEmpty()) {
            return true;
        }
        Connection conn = null;
        PreparedStatement pst = null;
        ResultSet ret = null;
        StringBuilder sql = null;
        try {
            int projectId = project.getIntValue(DataPullConstants.FULL_DATA_PULL_REQ_PROJECT_ID);
            int topoTableId = project.getIntValue(DataPullConstants.FULL_DATA_PULL_REQ_PROJECT_TOPO_TABLE_ID);
            conn = DBHelper.getDBusMgrConnection();
            sql = new StringBuilder();
            sql.append("select r.* from t_project_resource r ,t_project_topo_table t  ");
            sql.append("where r.table_id = t.table_id and r.project_id = ? and t.id = ? ");

            pst = conn.prepareStatement(sql.toString());
            pst.setInt(1, projectId);
            pst.setInt(2, topoTableId);
            ret = pst.executeQuery();
            if (ret.next()) {
                String fullpullEnableFlag = ret.getString("fullpull_enable_flag");
                if (fullpullEnableFlag != null && "1".equalsIgnoreCase(fullpullEnableFlag)) {
                    return true;
                }
            }

            //任务不安全,不能拉全量
            Long id = jsonObject.getJSONObject("payload").getLong(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_SEQNO);
            sql = new StringBuilder();
            sql.append("update t_fullpull_history h set h.state = 'abort',h.error_msg ='该任务对应的表不能拉全量,请联系管理员修改权限!'  where h.id = ?");
            pst = conn.prepareStatement(sql.toString());
            pst.setLong(1, id);
            pst.executeUpdate();
            LOG.error("任务对应的表不能拉全量. update state to abort id:{} \n. dataSourceInfo:{}", id, dataSourceInfo);
            return false;
        } catch (Exception e) {
            //出于安全考考虑,查询出错不能拉全量
            LOG.error("Exception Info:{}", e);
            return false;
        } finally {
            DBHelper.close(conn, pst, ret);
        }
    }

    /**
     * 只能有一个任务处于splitting,puliing的状态
     * 阻断增量的拉全量暂时没有好的处理办法,可能会有normal和indepent的任务同时处于splitting和pulling
     *
     * @param dataSourceInfo
     */
   private void waitForLastTaskEnd(String dataSourceInfo) {
        JSONObject jsonObject = JSONObject.parseObject(dataSourceInfo);
        Long id = jsonObject.getJSONObject("payload").getLong(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_SEQNO);
        Connection conn = null;
        PreparedStatement pst = null;
        ResultSet ret = null;
        try {
            conn = DBHelper.getDBusMgrConnection();
            StringBuilder sql = new StringBuilder();

            sql.append(" SELECT  p.id ,p.full_pull_req_msg_offset FROM ");
            sql.append("( SELECT  h.id ,h.full_pull_req_msg_offset,h.state FROM t_fullpull_history h ");
            if (isGlobal) {
                sql.append(" WHERE h.type = 'global'");
            } else {
                sql.append(" WHERE h.type = 'indepent' and h.dsName = '").append(dsName).append("'");
            }
            sql.append(" and h.id < ").append(id).append(" and h.full_pull_req_msg_offset is not NULL ");
            sql.append(" ORDER BY h.id DESC LIMIT 1 )p ").append(" where p.state not in ('ending','abort')");

            LOG.info("last task sql: {}",sql.toString());
            while (true) {
                pst = conn.prepareStatement(sql.toString());
                ret = pst.executeQuery();
                if (ret.next()) {
                    LOG.info("waitting for last task to end . id:{} offset:{}", ret.getLong("id"),ret.getLong("full_pull_req_msg_offset"));
                    TimeUnit.MILLISECONDS.sleep(60000);
                }else{
                    return;
                }
            }
        } catch (Exception e) {
            LOG.error("Exception Info:{}", e);
        } finally {
            DBHelper.close(conn, pst, ret);
        }
    }

    /*private void waitForLastTaskEnd(String dataSourceInfo) {
        JSONObject jsonObject = JSONObject.parseObject(dataSourceInfo);
        Long id = jsonObject.getJSONObject("payload").getLong(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_SEQNO);
        Connection conn = null;
        PreparedStatement pst = null;
        ResultSet ret = null;
        try {
            Long thisOffset = null;
            Long lastOffset = null;
            Long lastId = null;
            conn = DBHelper.getDBusMgrConnection();
            StringBuilder sql = new StringBuilder();

            sql.append(" SELECT id ,full_pull_req_msg_offset offset FROM t_fullpull_history WHERE full_pull_req_msg_offset is not NULL and id = ").append(id);
            sql.append(" UNION SELECT  p.id ,p.full_pull_req_msg_offset offset FROM ");
            sql.append("( SELECT  h.id ,h.full_pull_req_msg_offset,h.state FROM t_fullpull_history h ");
            if (isGlobal) {
                sql.append(" WHERE h.type = 'global'");
            } else {
                sql.append(" WHERE h.type = 'indepent' and h.dsName = '").append(dsName).append("'");
            }
            sql.append(" and h.id < ").append(id).append(" and h.full_pull_req_msg_offset is not NULL ");
            sql.append(" ORDER BY h.id DESC LIMIT 1 )p ").append(" where p.state not in ('ending','abort')");

            LOG.info("last task sql: {}",sql.toString());

            while (true) {
                pst = conn.prepareStatement(sql.toString());
                ret = pst.executeQuery();
                while (ret.next()) {
                    if (id.equals(ret.getLong("id"))) {
                        thisOffset = ret.getLong("offset");
                    } else {
                        lastId = ret.getLong("id");
                        lastOffset = ret.getLong("offset");
                    }
                }
                LOG.info("waitting for last task to end .thisId:{} ,thisOffset:{} ,lastId:{} , lastOffset:{}", id, thisOffset, lastId, lastOffset);
                //测试发现拉全量full_pull_req_msg_offset数值更新数据库比较慢,但是任务已经来了,就会导致取出的thisOffset是null
                if (thisOffset == null) {
                    //等待数据库完成update full_pull_req_msg_offset 并且提交了事务,才能查询到真正的full_pull_req_msg_offset数值
                    TimeUnit.MILLISECONDS.sleep(1000);
                    LOG.info("thisOffset is null");
                    continue;
                }
                if (lastOffset == null) {
                    return;
                }
                if (thisOffset < lastOffset) {
                    return;
                }
                TimeUnit.MILLISECONDS.sleep(60000);
                lastOffset = null;
                lastId = null;
            }
        } catch (Exception e) {
            LOG.error("Exception Info:{}", e);
        } finally {
            DBHelper.close(conn, pst, ret);
        }
    }*/

    /**
     * 定义字段id，该id在简单模式下没有用处，但在按照字段分组的模式下有很大的用处。
     * 该declarer变量有很大作用，我们还可以调用declarer.declareStream();来定义stramId，
     * 该id可以用来定义更加复杂的流拓扑结构
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("source"));
    }

    @Override
    public void ack(Object msgId) {
        try {
            if (msgId != null && ConsumerRecord.class.isInstance(msgId)) {
                ConsumerRecord<String, byte[]> record = getMessageId(msgId);
                this.flowedMsgCount--;
            }
            processedCount++;
            super.ack(msgId);
        } catch (Exception e) {
            LOG.error("DataSplittingSpout:Ack throwed exception!", e);
        }
    }

    @Override
    public void fail(Object msgId) {
        try {
            if (msgId != null && ConsumerRecord.class.isInstance(msgId)) {
                ConsumerRecord<String, byte[]> record = getMessageId(msgId);
                this.flowedMsgCount--;
            }
            processedCount++;
            super.fail(msgId);
        } catch (Exception e) {
            LOG.error("DataSplittingSpout:Fail ack throwed exception!", e);
        }
    }

    private <T> T getMessageId(Object msgId) {
        return (T) msgId;
    }

    private String increseBatchNo(String dataSourceInfo) {
        JSONObject wrapperObj = JSONObject.parseObject(dataSourceInfo);
        JSONObject payloadObj = wrapperObj.getJSONObject("payload");
        int dbusDatasourceId = payloadObj.getIntValue(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_DATA_SOURCE_ID);
        String targetTableName = payloadObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_TABLE_NAME);
        String schemaName = payloadObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_SCHEMA_NAME);


        String outputVersion = FullPullHelper.getOutputVersion();

        Connection conn = null;
        PreparedStatement pst = null;
        ResultSet ret = null;
        String version = "-1";
        String batchNo = "-1";
        String sql = "";
        try {
            conn = DBHelper.getDBusMgrConnection();

            //  传统全量：   1.2 拉全量自增版本， 1.3 拉全量自增batch_id
            //  独立拉全量： 1.2 没有，           1.3 不自增batch_id
            if (outputVersion.equals(Constants.VERSION_12)) {
                // 版本号是否加1标志位。如果没有此标志位(旧版消息没有)，将其置为true，表示要对版本号进行加1。
                if (!payloadObj.containsKey(DataPullConstants.FULL_DATA_PULL_REQ_INCREASE_VERSION)) {
                    payloadObj.put(DataPullConstants.FULL_DATA_PULL_REQ_INCREASE_VERSION, true);
                }

                if (payloadObj.getBooleanValue(DataPullConstants.FULL_DATA_PULL_REQ_INCREASE_VERSION)) {
                    // 增版本号标志位true时，执行。（传统的与增量有互动的全量拉取，每次拉取都需要增版本号。）
                    sql = "update t_meta_version set version=version + 1 where id = (SELECT ver_id from t_data_tables " +
                            "where ds_id = ? "
                            + "and schema_name = ? "
                            + "and table_name = ? )";
                    pst = conn.prepareStatement(sql);
                    pst.setInt(1, dbusDatasourceId);
                    pst.setString(2, schemaName);
                    pst.setString(3, targetTableName);
                    pst.execute();
                    pst.close();
                    pst = null;
                }
            } else {
                // 1.3 or later
                // 批次号是否加1标志位。如果没有此标志位(旧版消息没有)，将其置为true，表示要对批次号进行加1。
                if (!payloadObj.containsKey(DataPullConstants.FULL_DATA_PULL_REQ_INCREASE_BATCH_NO)) {
                    payloadObj.put(DataPullConstants.FULL_DATA_PULL_REQ_INCREASE_BATCH_NO, true);
                }
                if (payloadObj.getBooleanValue(DataPullConstants.FULL_DATA_PULL_REQ_INCREASE_BATCH_NO)) {
                    // 增批次号标志位为true时执行。（传统的与增量有互动的全量拉取，每次拉取都需要增批次号。）
                    sql = "update t_data_tables set batch_id = batch_id + 1 " +
                            "where ds_id = ? " +
                            "and schema_name = ? " +
                            "and table_name = ?";
                    pst = conn.prepareStatement(sql);
                    pst.setInt(1, dbusDatasourceId);
                    pst.setString(2, schemaName);
                    pst.setString(3, targetTableName);
                    pst.execute();
                    pst.close();
                    pst = null;
                }

                //查询batch id
                sql = "SELECT batch_id FROM t_data_tables " +
                        "where ds_id = ? " +
                        "and schema_name = ? " +
                        "and table_name = ? ";
                pst = conn.prepareStatement(sql);
                pst.setInt(1, dbusDatasourceId);
                pst.setString(2, schemaName);
                pst.setString(3, targetTableName);
                ret = pst.executeQuery();// 执行语句，得到结果集
                while (ret.next()) {
                    batchNo = ret.getString("batch_id");
                }
                ret.close();
                ret = null;
                pst.close();
                pst = null;
            }

            // 查询version，后续会写入zk
            sql = "SELECT mv.version FROM t_data_tables dt, t_meta_version mv " +
                    "where dt.ds_id = ? " +
                    "and dt.schema_name = ? " +
                    "and dt.table_name = ? " +
                    "and dt.ver_id = mv.id";
            pst = conn.prepareStatement(sql);
            pst.setInt(1, dbusDatasourceId);
            pst.setString(2, schemaName);
            pst.setString(3, targetTableName);
            // 执行语句，得到结果集
            ret = pst.executeQuery();
            while (ret.next()) {
                version = ret.getString("version");
            }

        } catch (Exception e) {
            LOG.error("Exception happened when try to get newest version from mysql db. Exception Info:", e);
        } finally {
            DBHelper.close(conn, pst, ret);
        }

        try {
            payloadObj.put(DataPullConstants.FullPullInterfaceJson.VERSION_KEY, Integer.parseInt(version));
            payloadObj.put(DataPullConstants.FullPullInterfaceJson.BATCH_NO_KEY, batchNo);
            wrapperObj.put(DataPullConstants.FullPullInterfaceJson.PAYLOAD_KEY, payloadObj);
            return wrapperObj.toJSONString();
        } catch (Exception e) {
            LOG.error("Apply new version failed.");
        }

        return dataSourceInfo;
    }

    private void loadRunningConf(String reloadMsgJson) {
        String notifyEvtName = reloadMsgJson == null ? "loaded" : "reloaded";
        try {
            //加载zk中的配置信息
            this.confMap = FullPullHelper.loadConfProps(zkConnect, topologyId, zkTopoRoot, Constants.ZkTopoConfForFullPull.FULL_PULL_SRC_TOPIC);
            this.MAX_FLOW_THRESHOLD = (Integer) confMap.get(FullPullHelper.RUNNING_CONF_KEY_MAX_FLOW_THRESHOLD);
            LOG.info("MAX_FLOW_THRESHOLD is {} on DataShardsSplittingSpout.loadRunningConf", MAX_FLOW_THRESHOLD);
            this.commonProps = (Properties) confMap.get(FullPullHelper.RUNNING_CONF_KEY_COMMON);
            this.dsName = commonProps.getProperty(Constants.ZkTopoConfForFullPull.DATASOURCE_NAME);
            this.fullPullSrcTopic = commonProps.getProperty(Constants.ZkTopoConfForFullPull.FULL_PULL_SRC_TOPIC);
            this.consumer = (Consumer<String, byte[]>) confMap.get(FullPullHelper.RUNNING_CONF_KEY_CONSUMER);
            this.zkService = (ZkService) confMap.get(FullPullHelper.RUNNING_CONF_KEY_ZK_SERVICE);

            LOG.info("Running Config is " + notifyEvtName + " successfully for DataShardsSplittingSpout!");
        } catch (Exception e) {
            LOG.error(notifyEvtName + "ing running configuration encountered Exception!", e);
        } finally {
            FullPullHelper.saveReloadStatus(reloadMsgJson, "splitting-spout", true, zkConnect);
        }
    }
}
