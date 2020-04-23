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


package com.creditease.dbus.helper;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.common.FullPullConstants;
import com.creditease.dbus.common.ZKMonitorHelper;
import com.creditease.dbus.common.bean.*;
import com.creditease.dbus.commons.*;
import com.creditease.dbus.commons.exception.InitializationException;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.handler.DefaultPullHandler;
import com.creditease.dbus.handler.DefaultSplitHandler;
import com.creditease.dbus.handler.PullHandler;
import com.creditease.dbus.handler.SplitHandler;
import com.creditease.dbus.manager.GenericSqlManager;
import com.creditease.dbus.manager.MySQLManager;
import com.creditease.dbus.manager.OracleManager;
import com.creditease.dbus.notopen.db2.DB2Manager;
import com.creditease.dbus.notopen.mongo.MongoPullHanlder;
import com.creditease.dbus.notopen.mongo.MongoSplitHandler;
import com.creditease.dbus.utils.JsonUtil;
import com.creditease.dbus.utils.TimeUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class FullPullHelper {
    private static Logger logger = LoggerFactory.getLogger(FullPullHelper.class);
    private static ConcurrentMap<String, DBConfiguration> dbConfMap = new ConcurrentHashMap<>();
    private static ConcurrentMap<String, Object> hdfsFileCacheMap = new ConcurrentHashMap<>();
    private static ThreadLocal<Map<Long, HdfsConnectInfo>> hdfsConnectMap = new ThreadLocal<>();

    private static volatile ZkService zkService = null;
    private static String splitterTopologyId;
    private static String pullerTopologyId;
    private static String zkConnect;
    private static boolean isGlobal;
    private static String zkMonitorRootNodePath;

    public static final String RUNNING_CONF_KEY_COMMON = "common";
    public static final String RUNNING_CONF_KEY_CONSUMER = "consumer";
    public static final String RUNNING_CONF_KEY_BYTE_PRODUCER = "byte.producer";
    public static final String RUNNING_CONF_KEY_STRING_PRODUCER = "string.producer";
    public static final String RUNNING_CONF_KEY_STRING_PRODUCER_PROPS = "string.producer.props";
    public static final String RUNNING_CONF_KEY_HDFS_CONF_PROPS = "hdfs.conf.props";
    public static final String RUNNING_CONF_KEY_ZK_SERVICE = "zkService";

    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");

    private static ThreadLocal<String> topoTypeHolder = new ThreadLocal<>();

    public static void setTopologyType(String topoType) {
        if (topoType.equalsIgnoreCase(FullPullConstants.FULL_SPLITTER_TYPE)) {
            topoTypeHolder.set(topoType);
        } else if (topoType.equalsIgnoreCase(FullPullConstants.FULL_PULLER_TYPE)) {
            topoTypeHolder.set(topoType);
        } else {
            throw new RuntimeException("setTopologyType(): bad topology type!! " + topoType);
        }
    }

    public static String getTopologyType() {
        String topoType = topoTypeHolder.get();
        if (topoType == null) {
            throw new RuntimeException("getTopologyType(): bad topology type!! " + topoType);
        } else {
            if (topoType.equalsIgnoreCase(FullPullConstants.FULL_SPLITTER_TYPE)) {
                return FullPullConstants.FULL_SPLITTER_TYPE;
            } else if (topoType.equalsIgnoreCase(FullPullConstants.FULL_PULLER_TYPE)) {
                return FullPullConstants.FULL_PULLER_TYPE;
            } else {
                throw new RuntimeException("getTopologyType(): bad topology type!! " + topoType);
            }
        }
    }

    private static void setTopologyId(String topoId) {
        String topoType = getTopologyType();
        if (topoType.equalsIgnoreCase(FullPullConstants.FULL_SPLITTER_TYPE)) {
            splitterTopologyId = topoId;
        } else if (topoType.equalsIgnoreCase(FullPullConstants.FULL_PULLER_TYPE)) {
            pullerTopologyId = topoId;
        } else {
            throw new RuntimeException("setTopolopyId(): bad topology type!! " + topoType);
        }
    }

    private static String getTopologyId() {
        String topoType = getTopologyType();
        if (topoType.equalsIgnoreCase(FullPullConstants.FULL_SPLITTER_TYPE)) {
            return splitterTopologyId;
        } else if (topoType.equalsIgnoreCase(FullPullConstants.FULL_PULLER_TYPE)) {
            return pullerTopologyId;
        } else {
            throw new RuntimeException("getTopologyId(): bad topology type!! " + topoType);
        }
    }


    public static synchronized void initialize(String topoId, String zkConString) {
        hdfsConnectMap.set(new HashMap<>());
        setTopologyId(topoId);
        zkConnect = zkConString;
        isGlobal = topoId.toLowerCase().indexOf(FullPullConstants.GLOBAL_FULLPULLER_TOPO_PREFIX) != -1 ? true : false;
        if (isGlobal) {
            zkMonitorRootNodePath = FullPullConstants.FULL_PULL_MONITOR_ROOT_GLOBAL;
        } else {
            zkMonitorRootNodePath = FullPullConstants.FULL_PULL_MONITOR_ROOT;
        }
    }

    /**
     * 获取指定配置文件的Properties对象
     *
     * @param reqString
     * @return 返回配置文件内容
     */
    public static synchronized DBConfiguration getDbConfiguration(String reqString) throws Exception {
        DBConfiguration dbConf = null;
        String topologyType = getTopologyType();
        Long id = getSeqNo(reqString);
        String key = topologyType + "_" + id;
        if (dbConfMap.containsKey(key)) {
            dbConf = dbConfMap.get(key);
        } else {
            dbConf = DBConfigurationHelper.generateDBConfiguration(reqString);
            dbConfMap.put(key, dbConf);
        }
        return dbConf;
    }

    /**
     * 获取hdfs的下一个文件名
     */
    public static synchronized String getHdfsFileName(String reqString) {
        String key = getSeqNo(reqString).toString();
        Integer index = (Integer) hdfsFileCacheMap.get(key);
        if (index == null) {
            index = 0;
        } else {
            index++;
        }
        String tail;
        if (index < 10) {
            tail = "000" + index;
        } else if (index < 100) {
            tail = "00" + index;
        } else if (index < 1000) {
            tail = "0" + index;
        } else {
            tail = index.toString();
        }
        hdfsFileCacheMap.put(key, index);
        return sdf.format(new Date()) + tail;
    }

    public static synchronized HdfsConnectInfo getHdfsConnectInfo(String reqString) {
        Long key = getSeqNo(reqString);
        return hdfsConnectMap.get().get(key);
    }

    public static synchronized void setHdfsConnectInfo(String reqString, HdfsConnectInfo hdfsConnectInfo) {
        Long key = getSeqNo(reqString);
        Map<Long, HdfsConnectInfo> connectInfoMap = hdfsConnectMap.get();
        connectInfoMap.put(key, hdfsConnectInfo);
        hdfsConnectMap.set(connectInfoMap);
    }

    /**
     * 发现dbConfMap会越来越大,每次任务完成清理一次
     */
    public static synchronized void clearDbConfMap(String reqString) {
        if (dbConfMap != null && dbConfMap.size() != 0) {
            for (String key : dbConfMap.keySet()) {
                if (key.endsWith(getSeqNo(reqString).toString())) {
                    dbConfMap.remove(key);
                }
            }
        }
        //保留七天的记录
        if (hdfsFileCacheMap != null && hdfsFileCacheMap.size() != 0) {
            Iterator<Map.Entry<String, Object>> iterator = hdfsFileCacheMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> next = iterator.next();
                long id = Long.parseLong(next.getKey());
                if ((TimeUtils.getCurrentTimeMillis() - id) > (1000 * 60 * 60 * 24 * 7)) {
                    iterator.remove();
                }
            }
        }
        //保留七天的记录
        Map<Long, HdfsConnectInfo> connectInfoMap = hdfsConnectMap.get();
        if (connectInfoMap != null && connectInfoMap.size() != 0) {
            Iterator<Map.Entry<Long, HdfsConnectInfo>> iterator = connectInfoMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Long, HdfsConnectInfo> next = iterator.next();
                if ((TimeUtils.getCurrentTimeMillis() - next.getKey()) > (1000 * 60 * 60 * 24 * 7)) {
                    iterator.remove();
                }
            }
        }
        hdfsConnectMap.set(connectInfoMap);
    }

    public static ZkService getZkService() {
        if (zkService != null) {
            return zkService;
        } else {
            synchronized (FullPullHelper.class) {
                if (zkService != null) {
                    return zkService;
                } else {
                    try {
                        zkService = new ZkService(zkConnect);
                    } catch (Exception e) {
                        logger.error("ZK connect failed. Exception:" + e);
                    }
                    return zkService;
                }
            }
        }
    }

    public static ProgressInfo getMonitorInfoFromZk(ZkService zkService, String path) throws Exception {
        int version = zkService.getVersion(path);
        if (version != -1) {
            byte[] data = zkService.getData(path);

            if (data != null) {
                ObjectMapper mapper = JsonUtil.getObjectMapper();
                ProgressInfo progressInfo = JsonUtil.convertToObject(mapper, new String(data), ProgressInfo.class);
                progressInfo.setZkVersion(version);
                return progressInfo;
            }
        }
        return (new ProgressInfo());
    }

    public static void finishPullReport(String reqString, String status, String errorMsg) throws Exception {
        JSONObject reqJson = JSONObject.parseObject(reqString);
        FullPullHistory fullPullHistory = new FullPullHistory();
        fullPullHistory.setId(getSeqNo(reqJson));
        fullPullHistory.setState(status);
        if (status != null && status.equals(FullPullConstants.FULL_PULL_STATUS_ENDING)) {
            fullPullHistory.setEndTime(TimeUtils.getCurrentTimeStampString());
        }
        if (errorMsg != null) {
            ProgressInfoParam infoParam = new ProgressInfoParam();
            infoParam.setErrorMsg(errorMsg);
            ProgressInfo progressInfo = FullPullHelper.updateZkNodeInfoWithVersion(zkService, FullPullHelper.getMonitorNodePath(reqString), infoParam);
            fullPullHistory.setErrorMsg(progressInfo.getErrorMsg());
        }
        //更新拉全量状态到历史记录表
        updateStatusToFullPullHistoryTable(fullPullHistory);
        //清空任务配置缓存
        clearDbConfMap(reqString);
    }

    public static String getTaskStateByHistoryTable(JSONObject reqJson) {
        Long id = getSeqNo(reqJson);

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = DBHelper.getDBusMgrConnection();
            StringBuilder sql = new StringBuilder();
            sql.append(" SELECT ");
            sql.append("     *");
            sql.append(" FROM ");
            sql.append("     t_fullpull_history");
            sql.append(" WHERE");
            sql.append("     id = ?");

            ps = conn.prepareStatement(sql.toString());
            ps.setLong(1, id);
            rs = ps.executeQuery();

            if (rs.next()) {
                return rs.getString("state");
            }
            return null;
        } catch (Exception e) {
            logger.error("getTaskStateByHistoryTable failed! Error message:{}", e);
            return null;
        } finally {
            try {
                DBHelper.close(conn, ps, rs);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    public static void updateStatusToFullPullHistoryTable(FullPullHistory fullPullHistory) throws Exception {
        Long id = fullPullHistory.getId();
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = DBHelper.getDBusMgrConnection();
            StringBuilder sql = new StringBuilder();
            sql.append(" SELECT ");
            sql.append("     *");
            sql.append(" FROM ");
            sql.append("     t_fullpull_history");
            sql.append(" WHERE");
            sql.append("     id = ?");

            ps = conn.prepareStatement(sql.toString());
            ps.setLong(1, id);
            rs = ps.executeQuery();

            ArrayList<Object> params = new ArrayList<>();
            if (rs.next()) {
                StringBuilder sb = new StringBuilder();
                sb.append("UPDATE t_fullpull_history SET ");
                String version = fullPullHistory.getVersion();
                if (StringUtils.isNotBlank(version)) {
                    sb.append("version = ?,");
                    params.add(version);
                }
                String batchId = fullPullHistory.getBatchId();
                if (StringUtils.isNotBlank(batchId)) {
                    sb.append("batch_id = ?,");
                    params.add(batchId);
                }
                String status = fullPullHistory.getState();
                if (StringUtils.isNotBlank(status)) {
                    String state = rs.getString("state");
                    status = getStateForUpdate(state, status);
                    sb.append("state = ?,");
                    params.add(status);
                }
                String errorMsg = fullPullHistory.getErrorMsg();
                if (StringUtils.isNotBlank(errorMsg)) {
                    sb.append("error_msg = ?,");
                    params.add(errorMsg);
                }
                String startSplitTime = fullPullHistory.getStartSplitTime();
                if (StringUtils.isNotBlank(startSplitTime)) {
                    sb.append("start_split_time = ?,");
                    params.add(startSplitTime);
                }
                String startPullTime = fullPullHistory.getStartPullTime();
                if (StringUtils.isNotBlank(startPullTime)) {
                    sb.append("start_pull_time = ?,");
                    params.add(startPullTime);
                }
                String endTime = fullPullHistory.getEndTime();
                if (StringUtils.isNotBlank(endTime)) {
                    sb.append("end_time = ?,");
                    params.add(endTime);
                }
                Long finishedPartitionCount = fullPullHistory.getFinishedPartitionCount();
                if (finishedPartitionCount != null) {
                    sb.append("finished_partition_count = ?,");
                    params.add(finishedPartitionCount);
                }
                Integer totalPartitionCount = fullPullHistory.getTotalPartitionCount();
                if (totalPartitionCount != null) {
                    sb.append("total_partition_count = ?,");
                    params.add(totalPartitionCount);
                }
                Long finishedRowCount = fullPullHistory.getFinishedRowCount();
                if (finishedRowCount != null) {
                    sb.append("finished_row_count = ?,");
                    params.add(finishedRowCount);
                }
                Long totalRowCount = fullPullHistory.getTotalRowCount();
                if (totalRowCount != null) {
                    sb.append("total_row_count = ?,");
                    params.add(totalRowCount);
                }
                Long firstShardMsgOffset = fullPullHistory.getFirstShardMsgOffset();
                if (firstShardMsgOffset != null) {
                    sb.append("first_shard_msg_offset = ?,");
                    params.add(firstShardMsgOffset);
                }
                Long lastShardMsgOffset = fullPullHistory.getLastShardMsgOffset();
                if (lastShardMsgOffset != null) {
                    sb.append("last_shard_msg_offset = ?,");
                    params.add(lastShardMsgOffset);
                }
                String splitColumn = fullPullHistory.getSplitColumn();
                if (StringUtils.isNotBlank(splitColumn)) {
                    sb.append("split_column = ?,");
                    params.add(splitColumn);
                }
                String fullpullCondition = fullPullHistory.getFullpullCondition();
                if (StringUtils.isNotBlank(fullpullCondition)) {
                    sb.append("fullpull_condition = ?,");
                    params.add(fullpullCondition);
                }

                Long currentShardOffset = fullPullHistory.getCurrentShardOffset();
                if (currentShardOffset != null) {
                    sb.append("current_shard_offset = ?,");
                    params.add(currentShardOffset);
                }
                sb.append("update_time = ? ");
                params.add(TimeUtils.getCurrentTimeStampString());

                sb.append(" where id=").append(id);
                logger.info("[t_fullpull_history] : {},\n param :{}", sb.toString(), params);
                ps = conn.prepareStatement(sb.toString());
                for (int i = 0; i < params.size(); i++) {
                    ps.setObject(i + 1, params.get(i));
                }
                ps.executeUpdate();
                conn.commit();
            } else {
                logger.error("Not find fullpull Record in t_fullpull_history! id:{} ", id);
            }
        } catch (Exception e) {
            if (conn != null) {
                conn.rollback();
            }
            logger.error("writeFullStatusToDbMgr failed! Error message:{}", e);
            throw e;
        } finally {
            try {
                DBHelper.close(conn, ps, rs);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }


    /**
     * 目前拉全量状态有init<splitting<pulling<ending<abort
     * 实践发现数据库状态更新并不按照理想状态更新,可能已经是pulling状态,又被更新为splitting
     * 针对这个情况按照init<splitting<pulling<ending<abort做一下处理
     */
    private static String getStateForUpdate(String oriStatus, String status) {
        if (StringUtils.isBlank(oriStatus) || StringUtils.isBlank(status)) {
            return status;
        }
        int oldState = getNumberByState(oriStatus);
        int newState = getNumberByState(status);
        if (oldState > newState) {
            return oriStatus;
        }
        return status;
    }

    private static int getNumberByState(String state) {
        int num = 0;
        switch (state) {
            case "init":
                num = 0;
                break;
            case "splitting":
                num = 1;
                break;
            case "pulling":
                num = 2;
                break;
            case "ending":
                num = 3;
                break;
            case "abort":
                num = 4;
        }
        return num;
    }

    public static synchronized ProgressInfo updateZkNodeInfoWithVersion(ZkService zkService, String path, ProgressInfoParam param) throws Exception {
        ProgressInfo progressInfo = getMonitorInfoFromZk(zkService, path);
        mergerProgressInfo(param, progressInfo);
        int nextVersion = updateZkNodeInfoWithVersion(zkService, path, progressInfo);
        while (nextVersion == -1) {
            logger.warn("更新zk监控出错,休息一下,再次更新! currentVersion: {}", progressInfo.getZkVersion());
            TimeUtils.sleep(500);
            progressInfo = getMonitorInfoFromZk(zkService, path);
            mergerProgressInfo(param, progressInfo);
            nextVersion = updateZkNodeInfoWithVersion(zkService, path, progressInfo);
            logger.warn("再次更新! newVersion: {}", progressInfo.getZkVersion());
        }
        return progressInfo;
    }

    public static void updateStatusToFullPullHistoryTable(ProgressInfo progressInfo, Long id, String startPullTime,
                                                          Long firstShardOffset, Long lastShardOffset) throws Exception {
        FullPullHistory fullPullHistory = new FullPullHistory();
        fullPullHistory.setId(id);
        fullPullHistory.setVersion(progressInfo.getVersion());
        fullPullHistory.setBatchId(progressInfo.getBatchNo());
        fullPullHistory.setState(progressInfo.getStatus());
        fullPullHistory.setErrorMsg(progressInfo.getErrorMsg());
        fullPullHistory.setStartSplitTime(progressInfo.getStartTime());
        fullPullHistory.setEndTime(progressInfo.getEndTime());
        if (StringUtils.isNotBlank(progressInfo.getFinishedCount())) {
            fullPullHistory.setFinishedPartitionCount(Long.parseLong(progressInfo.getFinishedCount()));
        }
        if (StringUtils.isNotBlank(progressInfo.getTotalCount())) {
            fullPullHistory.setTotalPartitionCount(Integer.parseInt(progressInfo.getTotalCount()));
        }
        if (StringUtils.isNotBlank(progressInfo.getFinishedRows())) {
            fullPullHistory.setFinishedRowCount(Long.parseLong(progressInfo.getFinishedRows()));
        }
        if (StringUtils.isNotBlank(progressInfo.getTotalRows())) {
            fullPullHistory.setTotalRowCount(Long.parseLong(progressInfo.getTotalRows()));
        }
        fullPullHistory.setFirstShardMsgOffset(firstShardOffset);
        fullPullHistory.setLastShardMsgOffset(lastShardOffset);
        fullPullHistory.setStartPullTime(startPullTime);
        FullPullHelper.updateStatusToFullPullHistoryTable(fullPullHistory);
    }

    private static void mergerProgressInfo(ProgressInfoParam param, ProgressInfo progressInfo) {
        Long partitions = param.getPartitions();
        if (partitions != null) {
            progressInfo.setPartitions(String.valueOf(partitions));
        }
        Long totalCount = param.getTotalCount();
        if (totalCount != null) {
            progressInfo.setTotalCount(String.valueOf(totalCount));
        }
        Long finishedCount = param.getFinishedCount();
        if (finishedCount != null) {
            finishedCount = progressInfo.getFinishedCount() == null ? finishedCount : (Long.parseLong(progressInfo.getFinishedCount()) + finishedCount);
            progressInfo.setFinishedCount(String.valueOf(finishedCount));
        }
        Long totalRows = param.getTotalRows();
        if (totalRows != null) {
            progressInfo.setTotalRows(String.valueOf(totalRows));
        }
        Long finishedRows = param.getFinishedRows();
        if (finishedRows != null) {
            finishedRows = progressInfo.getFinishedRows() == null ? finishedRows : (finishedRows + Long.parseLong(progressInfo.getFinishedRows()));
            progressInfo.setFinishedRows(String.valueOf(finishedRows));
        }
        Long startSecs = param.getStartSecs();
        if (startSecs != null) {
            progressInfo.setStartSecs(String.valueOf(startSecs));
        }
        String createTime = param.getCreateTime();
        if (StringUtils.isNotBlank(createTime)) {
            progressInfo.setCreateTime(createTime);
        }
        String startTime = param.getStartTime();
        if (StringUtils.isNotBlank(startTime)) {
            progressInfo.setStartTime(startTime);
        }
        String endTime = param.getEndTime();
        if (StringUtils.isNotBlank(endTime)) {
            progressInfo.setEndTime(endTime);
        }
        String errorMsg = param.getErrorMsg();
        if (StringUtils.isNotBlank(errorMsg)) {
            errorMsg = StringUtils.isBlank(progressInfo.getErrorMsg()) ? errorMsg : progressInfo.getErrorMsg() + ";" + errorMsg;
            progressInfo.setErrorMsg(errorMsg);
        }
        String status = param.getStatus();
        if (StringUtils.isNotBlank(status)) {
            status = getStateForUpdate(progressInfo.getStatus(), status);
            progressInfo.setStatus(status);
        }
        String version = param.getVersion();
        if (StringUtils.isNotBlank(version)) {
            progressInfo.setVersion(version);
        }
        String batchNo = param.getBatchNo();
        if (StringUtils.isNotBlank(batchNo)) {
            progressInfo.setBatchNo(batchNo);
        }
        String splitStatus = param.getSplitStatus();
        if (StringUtils.isNotBlank(splitStatus)) {
            progressInfo.setSplitStatus(splitStatus);
        }
        long curSecs = System.currentTimeMillis() / 1000;
        long start = progressInfo.getStartSecs() == null ? curSecs : Long.parseLong(progressInfo.getStartSecs());
        long consumeSecs = curSecs - start;
        progressInfo.setConsumeSecs(String.valueOf(consumeSecs) + "s");
        progressInfo.setUpdateTime(TimeUtils.getCurrentTimeStampString());
    }

    private static int updateZkNodeInfoWithVersion(ZkService zkService, String path, ProgressInfo progressInfo) throws Exception {
        if (zkService == null || StringUtils.isBlank(path)) {
            return -1;
        }
        if (!zkService.isExists(path)) {
            zkService.createNode(path, null);
        }
        ObjectMapper mapper = JsonUtil.getObjectMapper();
        String updateInfo = mapper.writeValueAsString(progressInfo);
        int nextVersion = zkService.setDataWithVersion(path, updateInfo.getBytes(), progressInfo.getZkVersion());
        if (nextVersion != -1) {
            progressInfo.setZkVersion(nextVersion);
        }
        return nextVersion;
    }

    public static void createProgressZkNode(ZkService zkService, DBConfiguration dbConf) {
        String projectName = (String) (dbConf.getConfProperties().get(FullPullConstants.REQ_PROJECT_NAME));
        Integer projectId = (Integer) (dbConf.getConfProperties().get(FullPullConstants.REQ_PROJECT_ID));
        String dbName = (String) (dbConf.getConfProperties().get(DBConfiguration.DataSourceInfo.DB_NAME));
        String dbSchema = (String) (dbConf.getConfProperties().get(DBConfiguration.DataSourceInfo.DB_SCHEMA));
        String tableName = (String) (dbConf.getConfProperties().get(DBConfiguration.DataSourceInfo.TABLE_NAME));
        String zkPath;
        try {
            zkPath = zkMonitorRootNodePath;
            if (!zkService.isExists(zkPath)) {
                zkService.createNode(zkPath, null);
            }
            if (StringUtils.isBlank(projectName)) {
                zkPath = buildZkPath(zkMonitorRootNodePath, dbName);
                if (!zkService.isExists(zkPath)) {
                    zkService.createNode(zkPath, null);
                }
                zkPath = buildZkPath(zkPath, dbSchema);
                if (!zkService.isExists(zkPath)) {
                    zkService.createNode(zkPath, null);
                }
                zkPath = buildZkPath(zkPath, tableName);
                if (!zkService.isExists(zkPath)) {
                    zkService.createNode(zkPath, null);
                }
            } else {
                zkPath = buildZkPath(zkMonitorRootNodePath, "Projects");
                if (!zkService.isExists(zkPath)) {
                    zkService.createNode(zkPath, null);
                }
                String projectsName = projectName + "_" + projectId;
                zkPath = buildZkPath(zkPath, projectsName);
                if (!zkService.isExists(zkPath)) {
                    zkService.createNode(zkPath, null);
                }
                zkPath = buildZkPath(zkPath, dbName);
                if (!zkService.isExists(zkPath)) {
                    zkService.createNode(zkPath, null);
                }
                zkPath = buildZkPath(zkPath, dbSchema);
                if (!zkService.isExists(zkPath)) {
                    zkService.createNode(zkPath, null);
                }
                zkPath = buildZkPath(zkPath, tableName);
                if (!zkService.isExists(zkPath)) {
                    zkService.createNode(zkPath, null);
                }
            }
        } catch (Exception e) {
            logger.error("create parent node failed exception!", e);
            throw new RuntimeException(e);
        }
    }


    private static String buildZkPath(String parent, String child) {
        return parent + "/" + child;
    }

    public static String getOutputVersion() {
        String outputVersion = getFullPullProperties(FullPullConstants.COMMON_CONFIG, true).getProperty(FullPullConstants.OUTPUT_VERSION);
        if (outputVersion == null) {
            outputVersion = Constants.VERSION_13;
        }
        return outputVersion;
    }

    public static String getMonitorNodePath(String reqString) throws Exception {
        JSONObject reqJson = JSONObject.parseObject(reqString);
        // 根据当前拉取全量的目标数据库信息，生成数据库表NameSpace信息
        DBConfiguration dbConf = getDbConfiguration(reqString);
        String dbNameSpace = dbConf.buildSlashedNameSpace(reqJson, getOutputVersion());
        return ZKMonitorHelper.getMonitorNodePath(zkMonitorRootNodePath, dbNameSpace, reqJson);
    }

    /**
     * 出错提前退出逻辑，如果取不到progress信息或者已经出现错误，跳过后来的tuple数据
     * 通过全量历史记录表改为abort跳过任务逻辑
     */
    public static Boolean hasErrorMessage(ZkService zkService, String reqString) throws Exception {
        ProgressInfo progressObj = getMonitorInfoFromZk(zkService, getMonitorNodePath(reqString));
        JSONObject reqJson = JSONObject.parseObject(reqString);
        Long id = getSeqNo(reqJson);
        String status = getTaskStateByHistoryTable(reqJson);
        if ((StringUtils.isNotBlank(status) && status.equalsIgnoreCase(FullPullConstants.FULL_PULL_STATUS_ABORT)) || progressObj.getErrorMsg() != null) {
            logger.error("Get process failed，skipped task:{},zk monitor errorMsg:{}, fullpullhistory status:{}", id, progressObj.getErrorMsg(), status);
            return true;
        }
        return false;
    }

    public static boolean validateMetaCompatible(String reqString) {
        OracleManager oracleManager = null;
        try {
            DBConfiguration dbConf = getDbConfiguration(reqString);
            // Mysql 不校验meta
            if (DbusDatasourceType.MYSQL.name().equalsIgnoreCase(dbConf.getString(DBConfiguration.DataSourceInfo.DS_TYPE))) {
                return true;
            }
            if (DbusDatasourceType.DB2.name().equalsIgnoreCase(dbConf.getString(DBConfiguration.DataSourceInfo.DS_TYPE))
                    || DbusDatasourceType.MONGO.name().equalsIgnoreCase(dbConf.getString(DBConfiguration.DataSourceInfo.DS_TYPE))) {
                return true;
            }
            logger.info("Not mysql. Should validate meta.");

            //从源库中获取meta信息
            oracleManager = (OracleManager) getDbManager(dbConf, dbConf.getString(DBConfiguration.DataSourceInfo.URL_PROPERTY_READ_ONLY));
            MetaWrapper metaInOriginalDb = oracleManager.queryMetaInOriginalDb();

            //从dbus manager库中获取meta信息
            MetaWrapper metaInDbus = DBHelper.getMetaInDbus(reqString);
            //从dbus manager库中获取不到meta信息，直接返回false
            if (metaInDbus == null) {
                logger.info("[Mysql manager] metaInDbus IS NULL.");
                return false;
            }
            //验证源库和管理库中的meta信息是否兼容
            return MetaWrapper.isCompatible(metaInOriginalDb, metaInDbus);
        } catch (Exception e) {
            logger.error("Failed on camparing meta.", e);
            return false;
        } finally {
            try {
                if (oracleManager != null) {
                    oracleManager.close();
                }
            } catch (Exception e) {
                logger.error("close dbManager error.", e);
            }
        }
    }

    public static GenericSqlManager getDbManager(DBConfiguration dbConf, String url) {
        String datasourceType = dbConf.getString(DBConfiguration.DataSourceInfo.DS_TYPE);
        DbusDatasourceType dataBaseType = DbusDatasourceType.valueOf(datasourceType.toUpperCase());
        switch (dataBaseType) {
            case ORACLE:
                return new OracleManager(dbConf, url);
            case MYSQL:
                return new MySQLManager(dbConf, url);
            case DB2:
                return new DB2Manager(dbConf, url);
            default:
                logger.error("the dataBaseType :{} is wrong", dataBaseType);
                throw new RuntimeException("the jdbcDriverClass is wrong");
        }
    }

    public static Properties getFullPullProperties(String confFileName, boolean useCacheIfCached) {
        String topologyId = getTopologyId();
        String customizeConfPath = topologyId + "/" + confFileName;
        logger.debug("customizeConfPath is {}", customizeConfPath);
        try {
            String topoType = getTopologyType();
            Properties properties = FullPullPropertiesHolder.getPropertiesInculdeCustomizeConf(topoType, confFileName, customizeConfPath, useCacheIfCached);
            if (FullPullConstants.CONSUMER_CONFIG.equals(confFileName)) {
                String clientId = topologyId + "-" + properties.getProperty("client.id");
                String groupId = topologyId + "-" + properties.getProperty("group.id");
                properties.put("client.id", clientId);
                properties.put("group.id", groupId);
                Properties globalConf = FullPullPropertiesHolder.getGlobalConf(topoType);
                properties.put("bootstrap.servers", globalConf.getProperty("bootstrap.servers"));
            }
            if (FullPullConstants.BYTE_PRODUCER_CONFIG.equals(confFileName) || FullPullConstants.STRING_PRODUCER_CONFIG.equals(confFileName)) {
                Properties globalConf = FullPullPropertiesHolder.getGlobalConf(topoType);
                properties.put("bootstrap.servers", globalConf.getProperty("bootstrap.servers"));
            }
            return properties;
        } catch (Exception e) {
            return new Properties();
        }
    }


    public static String getConfFromZk(String confFileName, String propKey, boolean useCacheIfCached) {
        Properties properties = getFullPullProperties(confFileName, useCacheIfCached);
        return properties.getProperty(propKey);
    }

    public static String getDataSourceKey(JSONObject reqJson) {
        StringBuilder key = new StringBuilder();
        JSONObject payloadJson = reqJson.getJSONObject(FullPullConstants.REQ_PAYLOAD);
        key.append(payloadJson.getString(FullPullConstants.REQ_PAYLOAD_SCHEMA_NAME)).append(".");
        key.append(payloadJson.getString(FullPullConstants.REQ_PAYLOAD_TABLE_NAME)).append("_");
        key.append(payloadJson.getString(FullPullConstants.REQ_PAYLOAD_SEQNO));
        return key.toString();
    }

    public static void updatePendingTasksToHistoryTable(String dsName, String opType, Consumer<String, byte[]> consumer, String topic) {
        String type = "";
        if (isGlobal) {
            type = "global";
        } else {
            type = "indepent";
        }
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            if (FullPullConstants.FULLPULL_PENDING_TASKS_OP_PULL_TOPOLOGY_RESTART.equals(opType)) {
                List<TopicPartition> list = Arrays.asList(new TopicPartition(topic, 0));
                consumer.seekToEnd(list);
                logger.info("[pull spout] restart :pulling consumer seek to end !");
            } else if (FullPullConstants.FULLPULL_PENDING_TASKS_OP_SPLIT_TOPOLOGY_RESTART.equals(opType)) {
                conn = DBHelper.getDBusMgrConnection();
                StringBuilder sql = new StringBuilder();
                sql.append("SELECT * FROM   ");
                sql.append("( SELECT h.* FROM t_fullpull_history h   ");
                sql.append("WHERE h.type = '").append(type).append("'");
                if (!isGlobal) {
                    sql.append(" and h.dsName = '").append(dsName).append("'");
                }
                sql.append(") h1 ");
                sql.append("WHERE h1.id >   ");
                sql.append("( SELECT h2.id FROM   ");
                sql.append("( SELECT h.* FROM t_fullpull_history h   ");
                sql.append("WHERE h.type = '").append(type).append("'");
                if (!isGlobal) {
                    sql.append(" and h.dsName = '").append(dsName).append("'");
                }
                sql.append(") h2 ");
                sql.append("WHERE h2.state = 'ending' OR h2.state = 'abort' ORDER BY h2.id DESC LIMIT 1 )   ");
                sql.append("ORDER BY h1.id LIMIT 2  ");

                logger.info("[split spout] restart :{}", sql.toString());
                ps = conn.prepareStatement(sql.toString());
                rs = ps.executeQuery();
                Long rowId1 = null;
                boolean pendingFlag = false;
                Long fullPullReqMsgOffset1 = null;
                Long fullPullReqMsgOffset2 = null;
                while (rs.next()) {
                    String state = rs.getString("state");
                    if (rs.getRow() == 1 && !("init").equals(state)) {
                        rowId1 = rs.getLong("id");
                        pendingFlag = true;
                    }
                    if (rs.getRow() == 1 && ("init").equals(state)) {
                        rowId1 = rs.getLong("id");
                        fullPullReqMsgOffset1 = rs.getLong(FullPullConstants.FULL_PULL_REQ_MSG_OFFSET);
                    }
                    if (rs.getRow() == 2) {
                        fullPullReqMsgOffset2 = rs.getLong(FullPullConstants.FULL_PULL_REQ_MSG_OFFSET);
                    }
                }
                DBHelper.close(null, ps, rs);

                //存在pending任务
                if (pendingFlag) {
                    if (fullPullReqMsgOffset2 != null) {
                        TopicPartition topicPartition = new TopicPartition(topic, 0);
                        consumer.seek(topicPartition, fullPullReqMsgOffset2);
                        logger.info("[split spout] restart : seek consumer to the first task and the state is init ! " +
                                "FULL_PULL_REQ_MSG_OFFSET :{}", fullPullReqMsgOffset2);
                    }

                    //pending任务状态置为abort
                    try {
                        conn = DBHelper.getDBusMgrConnection();
                        sql = new StringBuilder();
                        sql.append(" UPDATE ");
                        sql.append("     t_fullpull_history");
                        sql.append(" SET ");
                        sql.append("     state = 'abort', error_msg='全量程序重启,pending任务状态置为abort,该任务需要重新拉取!'");
                        sql.append(" WHERE id = ?");

                        ps = conn.prepareStatement(sql.toString());
                        ps.setLong(1, rowId1);
                        ps.executeUpdate();
                        conn.commit();
                    } catch (Exception e) {
                        if (conn != null) {
                            conn.rollback();
                        }
                        logger.error("Exception happpend when update pending task to abort", e);
                        throw e;
                    }

                    logger.info("[split spout] restart : find pening task ! set the state of pending task to abort! id:{}", rowId1);
                } else {
                    if (rowId1 != null) {
                        TopicPartition topicPartition = new TopicPartition(topic, 0);
                        consumer.seek(topicPartition, fullPullReqMsgOffset1);
                        logger.info("[split spout] restart : no pening task ! seek consumer to the first task and the state is init !" +
                                " FULL_PULL_REQ_MSG_OFFSET :{}!", fullPullReqMsgOffset1);
                    } else {
                        logger.info("[split spout] restart : no pening task ! seek consumer to end !");
                    }
                }
                DBHelper.close(null, ps, rs);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            try {
                DBHelper.close(conn, ps, rs);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    public static Map loadConfProps(String zkconnect, String topid, String dsName, String zkTopoRoot, String type) {
        Map confMap = new HashMap<>();
        try {
            //创建zk连接
            FullPullPropertiesHolder.initialize(getTopologyType(), zkconnect, zkTopoRoot);
            initialize(topid, zkconnect);
            //从zk中获取common-config节点的内容
            Properties commonProps = getFullPullProperties(FullPullConstants.COMMON_CONFIG, true);
            //创建zk连接，将其存入confMap中
            ZkService zkService = getZkService();
            confMap.put(RUNNING_CONF_KEY_COMMON, commonProps);
            confMap.put(RUNNING_CONF_KEY_ZK_SERVICE, zkService);
            String consumerSubTopic = null;
            if ("SplitSpout".equals(type)) {
                //String ctrlTopic = DBHelper.getCtrlTopicFromDB(dsName);
                //consumerSubTopic = ctrlTopic != null ? ctrlTopic : consumerSubTopic;
                consumerSubTopic = commonProps.getProperty(FullPullConstants.FULL_PULL_SRC_TOPIC);
            }
            if ("PullSpout".equals(type)) {
                consumerSubTopic = StringUtils.join(new String[]{commonProps.getProperty(FullPullConstants.FULL_PULL_MEDIANT_TOPIC),
                        commonProps.getProperty(FullPullConstants.FULL_PULL_CALLBACK_TOPIC)}, ",");
            }
            if (StringUtils.isNotBlank(consumerSubTopic)) {
                Properties consumerProps = getFullPullProperties(FullPullConstants.CONSUMER_CONFIG, true);
                Consumer<String, byte[]> consumer = createConsumer(consumerProps, consumerSubTopic);
                confMap.put(RUNNING_CONF_KEY_CONSUMER, consumer);
            }
            if ("SplitBolt".equals(type) || "PullProcessBolt".equals(type)) {
                Properties byteProducerProps = getFullPullProperties(FullPullConstants.BYTE_PRODUCER_CONFIG, true);
                if (null != byteProducerProps) {
                    Producer byteProducer = getProducer(byteProducerProps);
                    confMap.put(RUNNING_CONF_KEY_BYTE_PRODUCER, byteProducer);
                }
            }
            if ("PullBatchDataFetchBolt".equals(type)) {
                Properties stringProducerProps = getFullPullProperties(FullPullConstants.STRING_PRODUCER_CONFIG, true);
                if (null != stringProducerProps) {
                    Producer stringProducer = getProducer(stringProducerProps);
                    confMap.put(RUNNING_CONF_KEY_STRING_PRODUCER, stringProducer);
                    confMap.put(RUNNING_CONF_KEY_STRING_PRODUCER_PROPS, stringProducerProps);
                }
                confMap.put(RUNNING_CONF_KEY_HDFS_CONF_PROPS, getFullPullProperties(FullPullConstants.HDFS_CONFIG, true));
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new InitializationException();
        }
        return confMap;
    }

    public static Producer getProducer(Properties props) {
        return new KafkaProducer<>(props);
    }

    public static Consumer<String, byte[]> createConsumer(Properties props, String subscribeTopic) {
        List<TopicPartition> topicPartitions = new ArrayList<>();
        for (String topic : subscribeTopic.split(",")) {
            topicPartitions.add(new TopicPartition(topic, 0));
        }
        Consumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        consumer.assign(topicPartitions);
        consumer.seekToEnd(topicPartitions);
        return consumer;
    }

    public static void saveReloadStatus(String json, String title, boolean prepare, String zkServers) {
        if (json != null) {
            String msg = title + " reload successful!";
            ControlMessage message = ControlMessage.parse(json);
            CtlMessageResult result = new CtlMessageResult(title, msg);
            result.setOriginalMessage(message);
            CtlMessageResultSender sender = new CtlMessageResultSender(message.getType(), zkServers);
            sender.send(title, result, prepare, false);
            logger.info("Write reload status to zk successfully. Msg:{}", msg);
        }
    }

    public static SplitHandler getSplitHandler(String dsType) {
        DbusDatasourceType dataBaseType = DbusDatasourceType.valueOf(dsType.toUpperCase());
        switch (dataBaseType) {
            case ORACLE:
                return new DefaultSplitHandler();
            case MYSQL:
                return new DefaultSplitHandler();
            case DB2:
                return new DefaultSplitHandler();
            case MONGO:
                return new MongoSplitHandler();
            default:
                throw new RuntimeException("[split bolt] Wrong Database type.");
        }
    }

    public static PullHandler getPullHandler(String dsType) {
        DbusDatasourceType dataBaseType = DbusDatasourceType.valueOf(dsType.toUpperCase());
        switch (dataBaseType) {
            case ORACLE:
                return new DefaultPullHandler();
            case MYSQL:
                return new DefaultPullHandler();
            case DB2:
                return new DefaultPullHandler();
            case MONGO:
                return new MongoPullHanlder();
            default:
                throw new RuntimeException("[pull bolt] Wrong Database type.");
        }
    }

    public static Long getSeqNo(JSONObject reqJson) {
        return reqJson.getJSONObject(FullPullConstants.REQ_PAYLOAD).getLong(FullPullConstants.REQ_PAYLOAD_SEQNO);
    }

    public static Long getSeqNo(String reqString) {
        JSONObject reqJson = JSONObject.parseObject(reqString);
        return reqJson.getJSONObject(FullPullConstants.REQ_PAYLOAD).getLong(FullPullConstants.REQ_PAYLOAD_SEQNO);
    }


    public static Boolean isFinishedSplit(JSONObject reqJson) throws Exception {
        Long id = getSeqNo(reqJson);
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean result = true;
        try {
            conn = DBHelper.getDBusMgrConnection();
            StringBuilder sql = new StringBuilder();
            sql.append(" SELECT ");
            sql.append("     *");
            sql.append(" FROM ");
            sql.append("     t_fullpull_history");
            sql.append(" WHERE");
            sql.append("     id = ?");

            ps = conn.prepareStatement(sql.toString());
            ps.setLong(1, id);
            rs = ps.executeQuery();

            if (rs.next()) {
                Long offset = rs.getLong("last_shard_msg_offset");
                if (offset == null) {
                    result = false;
                }
            }
        } catch (Exception e) {
            if (conn != null) {
                conn.rollback();
            }
            logger.error("isFinishedSplit failed! Error message:{}", e);
            throw e;
        } finally {
            try {
                DBHelper.close(conn, ps, rs);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
        return result;
    }

}
