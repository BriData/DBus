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

package com.creditease.dbus.common;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import com.creditease.dbus.commons.*;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.common.utils.DBConfiguration;
import com.creditease.dbus.common.utils.JsonUtil;
import com.creditease.dbus.commons.Constants.ZkTopoConfForFullPull;
import com.creditease.dbus.commons.exception.InitializationException;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.manager.GenericJdbcManager;
import com.creditease.dbus.manager.MySQLManager;
import com.creditease.dbus.manager.OracleManager;
import com.esotericsoftware.minlog.Log;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FullPullHelper {
	private static Logger LOG = LoggerFactory.getLogger(FullPullHelper.class);
	private static ConcurrentMap<String, DBConfiguration> dbConfMap = new ConcurrentHashMap<>();

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
	public static final String RUNNING_CONF_KEY_MAX_FLOW_THRESHOLD = "maxFlowThreshold";
	public static final String RUNNING_CONF_KEY_ZK_SERVICE = "zkService";


	private static ThreadLocal<String> topoTypeHolder = new ThreadLocal<>();

	public static void setTopologyType(String topoType) {
		if (topoType.equalsIgnoreCase(Constants.FULL_SPLITTER_TYPE)) {
			topoTypeHolder.set(topoType);
		} else if (topoType.equalsIgnoreCase(Constants.FULL_PULLER_TYPE)) {
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
			if (topoType.equalsIgnoreCase(Constants.FULL_SPLITTER_TYPE)) {
				return Constants.FULL_SPLITTER_TYPE;
			} else if (topoType.equalsIgnoreCase(Constants.FULL_PULLER_TYPE)) {
				return Constants.FULL_PULLER_TYPE;
			} else {
				throw new RuntimeException("getTopologyType(): bad topology type!! " + topoType);
			}
		}
	}

	private static void setTopologyId(String topoId) {
		String topoType = getTopologyType();
		if (topoType.equalsIgnoreCase(Constants.FULL_SPLITTER_TYPE)) {
			splitterTopologyId = topoId;
		} else if (topoType.equalsIgnoreCase(Constants.FULL_PULLER_TYPE)) {
			pullerTopologyId = topoId;
		} else {
			throw new RuntimeException("setTopolopyId(): bad topology type!! " + topoType);
		}
	}

	private static String getTopologyId() {
		String topoType = getTopologyType();
		if (topoType.equalsIgnoreCase(Constants.FULL_SPLITTER_TYPE)) {
			return splitterTopologyId;
		} else if (topoType.equalsIgnoreCase(Constants.FULL_PULLER_TYPE)) {
			return pullerTopologyId;
		} else {
			throw new RuntimeException("getTopologyId(): bad topology type!! " + topoType);
		}
	}


	public static synchronized void initialize(String topoId, String zkConString) {
		setTopologyId(topoId);
		zkConnect = zkConString;
		isGlobal = topoId.toLowerCase().indexOf(ZkTopoConfForFullPull.GLOBAL_FULLPULLER_TOPO_PREFIX) != -1 ? true : false;
		if (isGlobal) {
			zkMonitorRootNodePath = Constants.FULL_PULL_MONITOR_ROOT_GLOBAL;
		} else {
			zkMonitorRootNodePath = Constants.FULL_PULL_MONITOR_ROOT;
		}
	}
//    public static void refreshCache() {
//        dbConfMap = new ConcurrentHashMap<>();
//        
//        if(!zkServiceMap.isEmpty()){
//            for (Map.Entry<String, ZkService> entry : zkServiceMap.entrySet()) { 
//                try {
//                    entry.getValue().close();
//                }
//                catch (IOException e) {
//                    LOG.warn("ZK connection close failed. Exception:"+e);
//                }
//            }
//        }
//        zkServiceMap = new ConcurrentHashMap<>();
//    }

	/**
	 * 获取指定配置文件的Properties对象
	 *
	 * @param dataSourceInfo 配置文件名称,不加扩展名
	 * @return 返回配置文件内容
	 */
	public static synchronized DBConfiguration getDbConfiguration(String dataSourceInfo) {
		DBConfiguration dbConf = null;
		String topologyType = getTopologyType();
		String key = topologyType + dataSourceInfo;
		if (dbConfMap.containsKey(key)) {
			dbConf = dbConfMap.get(key);
		} else {
			dbConf = DBHelper.generateDBConfiguration(dataSourceInfo);
			dbConfMap.put(key, dbConf);
		}
		return dbConf;
	}

	public static ZkService getZkService() throws Exception {
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
						LOG.error("ZK connect failed. Exception:" + e);
					}
					return zkService;
				}
			}
		}
	}


	public static boolean updateMonitorFinishPartitionInfo(ZkService zkService, String progressInfoNodePath, ProgressInfo objProgInfo) {
		try {
			for (int i = 0; i < 10; i++) {
				int nextVersion = FullPullHelper.updateZkNodeInfoWithVersion(zkService, progressInfoNodePath, objProgInfo);
				if (nextVersion != -1)
					break;

				//稍作休息重试
				Thread.sleep(200);
				ProgressInfo zkProgInfo = FullPullHelper.getMonitorInfoFromZk(zkService, progressInfoNodePath);
				objProgInfo.mergeProgressInfo(zkProgInfo);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.error("updateMonitorFinishPartitionInfo Failed.", e);
			return false;
		}
		return true;
	}

	public static void updateMonitorSplitPartitionInfo(ZkService zkService, String progressInfoNodePath, int totalShardsCount, long totalRows) {
		try {
			String currentTimeStampString = FullPullHelper.getCurrentTimeStampString();

			ProgressInfo progressObj = FullPullHelper.getMonitorInfoFromZk(zkService, progressInfoNodePath);
			progressObj.setUpdateTime(currentTimeStampString);
			progressObj.setTotalCount(String.valueOf(totalShardsCount));
			progressObj.setTotalRows(String.valueOf(totalRows));

			long curSecs = System.currentTimeMillis() / 1000;
			long startSecs = progressObj.getStartSecs() == null ? curSecs : Long.parseLong(progressObj.getStartSecs());
			long consumeSecs = curSecs - startSecs;
			progressObj.setConsumeSecs(String.valueOf(consumeSecs) + "s");

			ObjectMapper mapper = JsonUtil.getObjectMapper();
			FullPullHelper.updateZkNodeInfo(zkService, progressInfoNodePath, mapper.writeValueAsString(progressObj));
		} catch (Exception e) {
			LOG.error("Update Monitor Detail Info Failed.", e);
			throw new RuntimeException(e);
		}
	}

	public static ProgressInfo getMonitorInfoFromZk(ZkService zkService, String progressInfoNodePath) throws Exception {
		int version = zkService.getVersion(progressInfoNodePath);
		if (version != -1) {
			byte[] data = zkService.getData(progressInfoNodePath);

			if (data != null) {
				ObjectMapper mapper = JsonUtil.getObjectMapper();
				ProgressInfo progressObj = JsonUtil.convertToObject(mapper, new String(data), ProgressInfo.class);
				progressObj.setZkVersion(version);
				return progressObj;
			}
		}
		return (new ProgressInfo());
	}


	//有三种拉全量方法:
	// global pull 是独立拉全量，用于自我调试
	// 线属的非独立拉全量，停增量，  同一个topic
	// 线属的独立拉全量， 不停增量，补写zk，不写数据库， 别的topic
	public static boolean isIndenpentPull(boolean isGlobal, String dataSourceInfo) {
		if (isGlobal) {
			return true;
		} else {
			String dataType = JSONObject.parseObject(dataSourceInfo).getString("type");
			if (dataType == null) {
				LOG.error("dataType is null in isIndenpentPull()");
				return false;
			}
			if (dataType.equals(DataPullConstants.DATA_EVENT_INDEPENDENT_FULL_PULL_REQ)) {
				return true;
			} else {
				return false;
			}
		}
	}

	public static void startSplitReport(ZkService zkService, String dataSourceInfo) {
		try {
			String currentTimeStampString = FullPullHelper.getCurrentTimeStampString();
			long startSecs = System.currentTimeMillis() / 1000;

			JSONObject wrapperObj = JSONObject.parseObject(dataSourceInfo);
			JSONObject payloadObj = wrapperObj.getJSONObject("payload");
			String version = payloadObj.getString(DataPullConstants.FullPullInterfaceJson.VERSION_KEY);
			String batchNo = payloadObj.getString(DataPullConstants.FullPullInterfaceJson.BATCH_NO_KEY);

			DBConfiguration dbConf = FullPullHelper.getDbConfiguration(dataSourceInfo);
			ObjectMapper mapper = JsonUtil.getObjectMapper();
			FullPullHelper.createProgressZkNode(zkService, dbConf);

			//1 通知心跳程序 拉全量开始，心跳程序不在监控 增量数据
			if (!isIndenpentPull(isGlobal, dataSourceInfo)) {
				HeartBeatInfo heartBeanObj = new HeartBeatInfo();
				heartBeanObj.setArgs(dbConf.getDbNameAndSchema());
				heartBeanObj.setCmdType(String.valueOf(CommandType.FULL_PULLER_BEGIN.getCommand()));
				FullPullHelper.updateZkNodeInfo(zkService, Constants.HEARTBEAT_CONTROL_NODE, mapper.writeValueAsString(heartBeanObj));
			}

			//2 更新zk monitor节点信息
			ProgressInfo progressObj = new ProgressInfo();
			progressObj.setCreateTime(currentTimeStampString);
			progressObj.setStartTime(currentTimeStampString);
			progressObj.setUpdateTime(currentTimeStampString);
			progressObj.setStartSecs(String.valueOf(startSecs));
			progressObj.setStatus(Constants.FULL_PULL_STATUS_SPLITTING);
			progressObj.setVersion(version);
			progressObj.setBatchNo(batchNo);
			String progressInfoNodePath = FullPullHelper.getMonitorNodePath(dataSourceInfo);
			FullPullHelper.updateZkNodeInfo(zkService, progressInfoNodePath, mapper.writeValueAsString(progressObj));

			if (!isIndenpentPull(isGlobal, dataSourceInfo)) {
				//回写源端数据库状态
				FullPullHelper.updateFullPullReqTable(dataSourceInfo, FullPullHelper.getCurrentTimeStampString(), null, null, null);
			}
			//更新全量历史记录表,开始分片
			JSONObject fullpullUpdateParams = new JSONObject();
			fullpullUpdateParams.put("dataSourceInfo", dataSourceInfo);
			fullpullUpdateParams.put("status", "splitting");
			fullpullUpdateParams.put("start_split_time", FullPullHelper.getCurrentTimeStampString());
			FullPullHelper.writeStatusToDbManager(fullpullUpdateParams);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			String errorMsg = "Encountered exception when writing msg to zk monitor.";
			LOG.error(errorMsg, e);
			throw new RuntimeException(e);
		}
	}

	public static void finishPullReport(String dsName) throws Exception {
		//更新该数据源所有的监控节点信息为完成状态
		zkService = getZkService();
		List<String> paths = new ArrayList<>();
		if (isGlobal) {
			getMonitorPaths(zkMonitorRootNodePath, paths, zkService);
		} else {
			//非多租户监控
			String zkPath = zkMonitorRootNodePath + "/" + dsName;
			getMonitorPaths(zkPath, paths, zkService);
			//多租户监控
			zkPath = zkMonitorRootNodePath + "/Projects";
			for (String projectName : zkService.getChildren(zkPath)) {
				getMonitorPaths(zkPath + "/" + projectName + "/" + dsName, paths, zkService);
			}
		}
		String currentTimeStampString = FullPullHelper.getCurrentTimeStampString();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		for (String path : paths) {
			boolean updateFlag = true;
			ProgressInfo progressObj = FullPullHelper.getMonitorInfoFromZk(zkService, path);
			String status = progressObj.getStatus();
			String endTime = progressObj.getEndTime();
			if (status != null && org.apache.commons.lang3.StringUtils.isNotBlank(status.toString()) && (status.equals("ending") || status.equals("abort"))) {
				if (endTime != null && org.apache.commons.lang3.StringUtils.isNotBlank(endTime.toString())) {
					try {
						formatter.parse(endTime.toString());
						updateFlag = false;
					} catch (Exception e) {
						updateFlag = true;
					}
				} else {
					updateFlag = true;
				}
			} else {
				updateFlag = true;
			}

			if (updateFlag) {
				progressObj.setUpdateTime(currentTimeStampString);
				progressObj.setEndTime(currentTimeStampString);
				progressObj.setErrorMsg("全量程序重启,pending任务状态置为abort,该任务需要重新拉取!");
				progressObj.setStatus("abort");
				ObjectMapper mapper = JsonUtil.getObjectMapper();
				updateZkNodeInfo(zkService, path, mapper.writeValueAsString(progressObj));
			}
		}
	}

	private static void getMonitorPaths(String root, List<String> childrenPaths, ZkService zkService) throws Exception {
		List<String> children = null;
		try {
			if (!zkService.isExists(root)) return;
			children = zkService.getChildren(root);
			if (children != null && children.size() > 0) {
				for (String nodeName : children) {
					getMonitorPaths(root + "/" + nodeName, childrenPaths, zkService);
				}
			}
			if (root.contains(" - ")) {
				childrenPaths.add(root);
			}
		} catch (Exception e) {
			throw e;
		}
	}

	//拉取任务结束,更新任务状态到zk,monitor,数据库
	public static void finishPullReport(ZkService zkService, String dataSourceInfo, String completedTime, String pullStatus, String errorMsg) {
		DBConfiguration dbConf = getDbConfiguration(dataSourceInfo);
		// 通知相关方全量拉取完成：包括以下几个部分。
		try {
			// 一、向zk监控信息写入完成
			if (!isIndenpentPull(isGlobal, dataSourceInfo)) {
				HeartBeatInfo heartBeanObj = new HeartBeatInfo();
				heartBeanObj.setArgs(dbConf.getDbNameAndSchema());
				heartBeanObj.setCmdType(String.valueOf(CommandType.FULL_PULLER_END.getCommand()));

				ObjectMapper mapper = JsonUtil.getObjectMapper();
				updateZkNodeInfo(zkService, Constants.HEARTBEAT_CONTROL_NODE, mapper.writeValueAsString(heartBeanObj));
			}

			// 二、通知monitorRoot节点和monitor节点
			if (!pullStatus.equals(Constants.DataTableStatus.DATA_STATUS_OK)) {
				sendErrorMsgToZkMonitor(zkService, dataSourceInfo, errorMsg);
			}

			if (!isIndenpentPull(isGlobal, dataSourceInfo)) {
				// 三、将完成时间回写到数据库，通知业务方
				GenericJdbcManager dbManager = null;
				try {
					dbManager = FullPullHelper.getDbManager(dbConf, dbConf.getString(DBConfiguration.DataSourceInfo.URL_PROPERTY_READ_WRITE));
					dbManager.writeBackOriginalDb(null, completedTime, pullStatus, errorMsg);
				} catch (Exception e) {
					Log.error("Error occurred when report full data pulling status to original DB.");
					pullStatus = Constants.DataTableStatus.DATA_STATUS_ABORT;
				} finally {
					try {
						if (dbManager != null) {
							dbManager.close();
						}
					} catch (Exception e) {
						LOG.error("close dbManager error.", e);
					}
				}
			}

			if (!isIndenpentPull(isGlobal, dataSourceInfo)) {
				// 四、向约定topic发送全量拉取完成或出错终止信息，供增量拉取进程使用
				sendAckInfoToCtrlTopic(dataSourceInfo, completedTime, pullStatus);
			}

		} catch (Exception e) {
			LOG.error("finishPullReport() Exception:", e);
			throw new RuntimeException(e);
		}
		JSONObject fullpullUpdateParams = new JSONObject();
		fullpullUpdateParams.put("dataSourceInfo", dataSourceInfo);
		fullpullUpdateParams.put("status", pullStatus);
		fullpullUpdateParams.put("error_msg", errorMsg);
		fullpullUpdateParams.put("end_time", completedTime);
		writeStatusToDbManager(fullpullUpdateParams);
	}

	public static String getTaskStateByHistoryTable(JSONObject dataSourceInfo) throws Exception {
		Long id = dataSourceInfo.getLong(DataPullConstants.FullPullInterfaceJson.ID_KEY);

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
			LOG.error("getTaskStateByHistoryTable failed! Error message:{}", e);
			return null;
		} finally {
			try {
				DBHelper.close(conn, ps, rs);
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
			}
		}
	}

	public static void writeStatusToDbManager(JSONObject fullpullUpdateParams) {
		LOG.info("fullpullUpdate param:{}", fullpullUpdateParams);
		JSONObject dsInfo = fullpullUpdateParams.getJSONObject("dataSourceInfo");
		Long id = dsInfo.getLong(DataPullConstants.FullPullInterfaceJson.ID_KEY);

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
				StringBuilder sb = new StringBuilder();
				sb.append("UPDATE t_fullpull_history SET ");
				Object version = fullpullUpdateParams.get("version");
				if (version != null) {
					sb.append("version=").append(version).append(",");
				}
				Object batch_id = fullpullUpdateParams.get("batch_id");
				if (batch_id != null) {
					sb.append("batch_id=").append(batch_id).append(",");
				}
				Object status = fullpullUpdateParams.get("status");
				if (status != null) {
					if ("ok".equals(status)) {
						status = "ending";
					}
					String state = rs.getString("state");
					status = getStateForUpdate(state, (String) status);
					sb.append("state='").append(status).append("',");
				}
				Object error_msg = fullpullUpdateParams.get("error_msg");
				if (error_msg != null) {
					String errorMsg = (String) error_msg;
					errorMsg = errorMsg.replace("\\'", "'");
					errorMsg = errorMsg.replace("'", "\\'");
					sb.append("error_msg='").append(errorMsg).append("',");
				}
				Object start_split_time = fullpullUpdateParams.get("start_split_time");
				if (start_split_time != null) {
					sb.append("start_split_time='").append(start_split_time).append("',");
				}
				Object start_pull_time = fullpullUpdateParams.get("start_pull_time");
				if (start_pull_time != null) {
					sb.append("start_pull_time='").append(start_pull_time).append("',");
				}
				Object end_time = fullpullUpdateParams.get("end_time");
				if (end_time != null) {
					sb.append("end_time='").append(end_time).append("',");
				}
				Object finished_partition_count = fullpullUpdateParams.get("finished_partition_count");
				if (finished_partition_count != null) {
					sb.append("finished_partition_count=").append(finished_partition_count).append(",");
				}
				Object total_partition_count = fullpullUpdateParams.get("total_partition_count");
				if (total_partition_count != null) {
					sb.append("total_partition_count=").append(total_partition_count).append(",");
				}
				Object finished_row_count = fullpullUpdateParams.get("finished_row_count");
				if (finished_row_count != null) {
					sb.append("finished_row_count=").append(finished_row_count).append(",");
				}
				Object total_row_count = fullpullUpdateParams.get("total_row_count");
				if (total_row_count != null) {
					sb.append("total_row_count=").append(total_row_count).append(",");
				}
				Object first_shard_msg_offset = fullpullUpdateParams.get("first_shard_msg_offset");
				if (first_shard_msg_offset != null) {
					sb.append("first_shard_msg_offset=").append(first_shard_msg_offset).append(",");
				}
				Object last_shard_msg_offset = fullpullUpdateParams.get("last_shard_msg_offset");
				if (last_shard_msg_offset != null) {
					sb.append("last_shard_msg_offset=").append(last_shard_msg_offset).append(",");
				}
				Object split_column = fullpullUpdateParams.get("split_column");
				if (split_column != null) {
					sb.append("split_column='").append(split_column).append("',");
				}
				sb.append("update_time='").append(FullPullHelper.getCurrentTimeStampString()).append("'");
				sb.append(" where id=").append(id);
				LOG.info("fullpullUpdate sql:{}", sb.toString());
				ps = conn.prepareStatement(sb.toString());
				ps.executeUpdate();
			} else {
				LOG.error("No find fullpull Record in t_fullpull_history! dataSourceInfo:{}", dsInfo);
			}
		} catch (Exception e) {
			LOG.error("writeFullStatusToDbMgr failed! Error message:{}", e);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (ps != null)
					ps.close();
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				LOG.error(e.getMessage(), e);
			}
		}
	}

	/**
	 * 目前拉全量状态有init<splitting<pulling<ending<abort
	 * 实践发现数据库状态更新并不按照理想状态更新,可能已经是pulling状态,又被更新为splitting
	 * 针对这个情况按照init<splitting<pulling<ending<abort做一下处理
	 *
	 * @param state
	 * @param status
	 * @return
	 */
	private static String getStateForUpdate(String state, String status) {
		int oldState = getNumberByState(state);
		int newState = getNumberByState(status);
		if (oldState > newState) {
			return state;
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

	private static String getCostTime(Date startTime) {
		long l = System.currentTimeMillis() - startTime.getTime();
		long lm = l / 60000;
		return lm / 60 + "h " + lm % 60 + "m";
	}

	private static void sendErrorMsgToZkMonitor(ZkService zkService, String dataSourceInfo, String errorMsg) throws Exception {
		String currentTimeStampString = FullPullHelper.getCurrentTimeStampString();

		String progressInfoNodePath = FullPullHelper.getMonitorNodePath(dataSourceInfo);
		ProgressInfo progressObj = FullPullHelper.getMonitorInfoFromZk(zkService, progressInfoNodePath);
		progressObj.setUpdateTime(currentTimeStampString);
		String oriErrorMsg = progressObj.getErrorMsg();
		errorMsg = StringUtils.isBlank(oriErrorMsg) ? errorMsg : oriErrorMsg + ";" + errorMsg;
		progressObj.setErrorMsg(errorMsg);

		ObjectMapper mapper = JsonUtil.getObjectMapper();
		updateZkNodeInfo(zkService, progressInfoNodePath, mapper.writeValueAsString(progressObj));
	}

	public static void updateZkNodeInfo(ZkService zkService, String zkPath, String updateInfo) throws Exception {
		if (zkService == null || StringUtils.isBlank(zkPath) || StringUtils.isBlank(updateInfo)) {
			return;
		}
		if (!zkService.isExists(zkPath)) {
			zkService.createNode(zkPath, null);
		}
		zkService.setData(zkPath, updateInfo.getBytes());
	}

	public static int updateZkNodeInfoWithVersion(ZkService zkService, String progressInfoNodePath, ProgressInfo objProgInfo) throws Exception {
		if (zkService == null || StringUtils.isBlank(progressInfoNodePath)) {
			return -1;
		}

		if (!zkService.isExists(progressInfoNodePath)) {
			zkService.createNode(progressInfoNodePath, null);
		}

		ObjectMapper mapper = JsonUtil.getObjectMapper();
		String updateInfo = mapper.writeValueAsString(objProgInfo);
		int nextVersion = zkService.setDataWithVersion(progressInfoNodePath, updateInfo.getBytes(), objProgInfo.getZkVersion());
		if (nextVersion != -1) {
			objProgInfo.setZkVersion(nextVersion);
		}
		return nextVersion;
	}


	public static void createProgressZkNode(ZkService zkService, DBConfiguration dbConf) {
		String projectName = (String) (dbConf.getConfProperties().get(DataPullConstants.FULL_DATA_PULL_REQ_PROJECT_NAME));
		Integer projectId = (Integer) (dbConf.getConfProperties().get(DataPullConstants.FULL_DATA_PULL_REQ_PROJECT_ID));
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
				zkPath = buildZkPath(zkMonitorRootNodePath, dbName + "/" + dbSchema);
				if (!zkService.isExists(zkPath)) {
					zkService.createNode(zkPath, null);
				}
				zkPath = buildZkPath(zkMonitorRootNodePath, dbName + "/" + dbSchema + "/" + tableName);
				if (!zkService.isExists(zkPath)) {
					zkService.createNode(zkPath, null);
				}
			} else {
				zkPath = buildZkPath(zkMonitorRootNodePath, "Projects");
				if (!zkService.isExists(zkPath)) {
					zkService.createNode(zkPath, null);
				}
				String projectsName = projectName + "_" + projectId;
				zkPath = buildZkPath(zkMonitorRootNodePath, "Projects/" + projectsName);
				if (!zkService.isExists(zkPath)) {
					zkService.createNode(zkPath, null);
				}
				zkPath = buildZkPath(zkMonitorRootNodePath, "Projects/" + projectsName + "/" + dbName);
				if (!zkService.isExists(zkPath)) {
					zkService.createNode(zkPath, null);
				}
				zkPath = buildZkPath(zkMonitorRootNodePath, "Projects/" + projectsName + "/" + dbName + "/" + dbSchema);
				if (!zkService.isExists(zkPath)) {
					zkService.createNode(zkPath, null);
				}
				zkPath = buildZkPath(zkMonitorRootNodePath, "Projects/" + projectsName + "/" + dbName + "/" + dbSchema + "/" + tableName);
				if (!zkService.isExists(zkPath)) {
					zkService.createNode(zkPath, null);
				}
			}
		} catch (Exception e) {
			Log.error("create parent node failed exception!", e);
			throw new RuntimeException(e);
		}
	}

	//TODO 应该放到 util 类中
	public static String getCurrentTimeStampString() {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		String timeStampString = formatter.format(new Date());
		return timeStampString;
	}

	private static String buildZkPath(String parent, String child) {
		return parent + "/" + child;
	}


	public static String getOutputVersion() {
		String outputVersion = getFullPullProperties(Constants.ZkTopoConfForFullPull.COMMON_CONFIG, true)
				.getProperty(Constants.ZkTopoConfForFullPull.OUTPUT_VERSION);
		if (outputVersion == null) {
			outputVersion = Constants.VERSION_13;
		}
		return outputVersion;
	}

	public static String getMonitorNodePath(String dataSourceInfo) {
		String outputVersion = getOutputVersion();
		LOG.info("getMonitorNodePath: " + outputVersion);


		// 根据当前拉取全量的目标数据库信息，生成数据库表NameSpace信息
		DBConfiguration dbConf = FullPullHelper.getDbConfiguration(dataSourceInfo);
		String dbNameSpace = dbConf.buildSlashedNameSpace(dataSourceInfo, outputVersion);

		String monitorNodePath = "";
		JSONObject dataSourceInfoJson = JSONObject.parseObject(dataSourceInfo);
		JSONObject projectJson = dataSourceInfoJson.getJSONObject("project");
		// 获得全量拉取监控信息的zk node path
		if (projectJson != null && !projectJson.isEmpty()) {
			monitorNodePath = FullPullHelper.buildKeeperZkPath(zkMonitorRootNodePath, dbNameSpace, projectJson);
		} else {
			monitorNodePath = FullPullHelper.buildZkPath(zkMonitorRootNodePath, dbNameSpace);
		}
		return monitorNodePath;
	}

	private static String buildKeeperZkPath(String zkMonitorRootNodePath, String dbNameSpace, JSONObject projectJson) {
		int projectId = projectJson.getIntValue(DataPullConstants.FULL_DATA_PULL_REQ_PROJECT_ID);
		String projectName = projectJson.getString(DataPullConstants.FULL_DATA_PULL_REQ_PROJECT_NAME);
		StringBuilder sb = new StringBuilder();
		sb.append(zkMonitorRootNodePath).append("/Projects/")
				.append(projectName).append("_").append(projectId)
				.append("/").append(dbNameSpace);
		return sb.toString();
	}

//    @Deprecated
//    public static String getDbNameSpace(String dataSourceInfo) {
//        DBConfiguration dbConf = FullPullHelper.getDbConfiguration(dataSourceInfo);
//        String dbNameSpace = dbConf.buildSlashedNameSpace(dataSourceInfo);
//        return dbNameSpace;
//    }

	public static boolean validateMetaCompatible(String dataSourceInfo) {
		OracleManager oracleManager = null;
		try {
			DBConfiguration dbConf = getDbConfiguration(dataSourceInfo);
			// Mysql 不校验meta
			if (DbusDatasourceType.MYSQL.name().equalsIgnoreCase(dbConf.getString(DBConfiguration.DataSourceInfo.DS_TYPE))) {
				return true;
			}
			LOG.info("Not mysql. Should validate meta.");

			//从源库中获取meta信息
			oracleManager = (OracleManager) FullPullHelper.getDbManager(dbConf, dbConf.getString(DBConfiguration.DataSourceInfo.URL_PROPERTY_READ_ONLY));
			MetaWrapper metaInOriginalDb = oracleManager.queryMetaInOriginalDb();

			//从dbus manager库中获取meta信息
			MetaWrapper metaInDbus = DBHelper.getMetaInDbus(dataSourceInfo);

			//从dbus manager库中获取不到meta信息，直接返回false
			if (metaInDbus == null) {
				LOG.info("[Mysql manager] metaInDbus IS NULL.");
				return false;
			}

			//验证源库和管理库中的meta信息是否兼容
			return MetaWrapper.isCompatible(metaInOriginalDb, metaInDbus);
		} catch (Exception e) {
			LOG.error("Failed on camparing meta.", e);
			return false;
		} finally {
			try {
				if (oracleManager != null) {
					oracleManager.close();
				}
			} catch (Exception e) {
				LOG.error("close dbManager error.", e);
			}
		}
	}


	private static void sendAckInfoToCtrlTopic(String dataSourceInfo, String completedTime, String pullStatus) {
		try {
			// 在源dataSourceInfo的基础上，更新全量拉取相关信息。然后发回src topic
			JSONObject jsonObj = JSONObject.parseObject(dataSourceInfo);
			jsonObj.put(DataPullConstants.FullPullInterfaceJson.FROM_KEY, DataPullConstants.FullPullInterfaceJson.FROM_VALUE);
			jsonObj.put(DataPullConstants.FullPullInterfaceJson.TYPE_KEY, DataPullConstants.FullPullInterfaceJson.TYPE_VALUE);
			// notifyFullPullRequestor
			JSONObject payloadObj = jsonObj.getJSONObject(DataPullConstants.FullPullInterfaceJson.PAYLOAD_KEY);
			// 完成时间
			payloadObj.put(DataPullConstants.FullPullInterfaceJson.COMPLETE_TIME_KEY, completedTime);
			// 拉取是否成功标志位
			payloadObj.put(DataPullConstants.FullPullInterfaceJson.DATA_STATUS_KEY, pullStatus);
			jsonObj.put(DataPullConstants.FullPullInterfaceJson.PAYLOAD_KEY, payloadObj);
			String ctrlTopic = getFullPullProperties(Constants.ZkTopoConfForFullPull.COMMON_CONFIG, true)
					.getProperty(Constants.ZkTopoConfForFullPull.FULL_PULL_SRC_TOPIC);
			Producer producer = DbusHelper
					.getProducer(getFullPullProperties(Constants.ZkTopoConfForFullPull.BYTE_PRODUCER_CONFIG, true));
			ProducerRecord record = new ProducerRecord<>(ctrlTopic, DataPullConstants.FullPullInterfaceJson.TYPE_VALUE, jsonObj.toString().getBytes());
			Future<RecordMetadata> future = producer.send(record);
			RecordMetadata meta = future.get();
		} catch (Exception e) {
			Log.error("Error occurred when report full data pulling status.", e);
			throw new RuntimeException(e);
		}
	}

	public static GenericJdbcManager getDbManager(DBConfiguration dbConf, String url) {
		String datasourceType = dbConf.getString(DBConfiguration.DataSourceInfo.DS_TYPE);
		String dbDriverClass = DbusDatasourceType.getDataBaseDriverClass(DbusDatasourceType.valueOf(datasourceType.toUpperCase()));
		DbusDatasourceType dataBaseType = DbusDatasourceType.valueOf(datasourceType.toUpperCase());
		switch (dataBaseType) {
			case ORACLE:
				return new OracleManager(dbConf, url);
			case MYSQL:
				return new MySQLManager(dbConf, url);
			default:
				return new GenericJdbcManager(dbDriverClass, dbConf, url);
		}

	}

//    private static String buildOracleManagerCacheKey(String dataSourceInfo, String url){
//        return dataSourceInfo+url;
//    }
    
 /*   public static  synchronized void closeOracleManager(String dataSourceInfo, String url) {
        String key = buildOracleManagerCacheKey(dataSourceInfo, url);
        OracleManager oracleManager = oracleManagerMap.get(key);
        if(oracleManager!=null){
            try {
                oracleManager.close();
                Iterator iterator = oracleManagerMap.keySet().iterator();   
                while (iterator.hasNext()) {  
                    String oracleManagerKey = (String) iterator.next();  
                    if (key.equals(oracleManagerKey)) {  
                       iterator.remove();// 必须有，否则抛 java.util.ConcurrentModificationException 异常哦
                       oracleManagerMap.remove(key);      
                     }  
                 }  
            }
            catch (SQLException e) {
                // TODO Auto-generated catch block
                LOG.error(e.getMessage(),e);
            }
            catch (Exception e) {
                // TODO Auto-generated catch block
                LOG.error(e.getMessage(),e);
            }
        }
    }*/


	/**
	 * 更新数据库全量拉取请求表
	 */
	public static void updateFullPullReqTable(String dataSourceInfo, String startTime, String completedTime, String pullStatus, String errorMsg) {
		GenericJdbcManager dbManager = null;
		try {
			DBConfiguration dbConf = FullPullHelper.getDbConfiguration(dataSourceInfo);
			String readWriteDbUrl = dbConf.getString(DBConfiguration.DataSourceInfo.URL_PROPERTY_READ_WRITE);
			dbManager = getDbManager(dbConf, readWriteDbUrl);
			dbManager.writeBackOriginalDb(startTime, completedTime, pullStatus, errorMsg);
		} catch (Exception e) {
			LOG.error("Error occured when report full data pulling status to original DB.");
		} finally {
			try {
				if (dbManager != null) {
					dbManager.close();
				}
			} catch (Exception e) {
				LOG.error("close dbManager error.", e);
			}
		}
	}

	public static Properties getFullPullProperties(String confFileName, boolean useCacheIfCached) {
		String topologyId = getTopologyId();
		String customizeConfPath = topologyId + "/" + confFileName;
		LOG.debug("customizeConfPath is {}", customizeConfPath);
		try {
			String topoType = getTopologyType();
			String path = FullPullPropertiesHolder.getPath(topoType);
			Properties properties = FullPullPropertiesHolder.getPropertiesInculdeCustomizeConf(topoType, confFileName, customizeConfPath, useCacheIfCached);
			if (Constants.ZkTopoConfForFullPull.CONSUMER_CONFIG.equals(confFileName)) {
				String clientId = topologyId + "_" + properties.getProperty("client.id");
				String groupId = topologyId + "_" + properties.getProperty("group.id");
				properties.put("client.id", clientId);
				properties.put("group.id", groupId);
				LOG.warn("typoType: {}, path: {}, client.id: {}, group.id: {}", topoType, path, clientId, groupId);
			}
			if (Constants.ZkTopoConfForFullPull.BYTE_PRODUCER_CONFIG.equals(confFileName)) {
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

	public static String getDataSourceKey(JSONObject ds) {
		StringBuilder key = new StringBuilder();
		JSONObject jsonObj = ds.getJSONObject(DataPullConstants.FullPullInterfaceJson.PAYLOAD_KEY);
		key.append(jsonObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_SCHEMA_NAME)).append("_");
		key.append(jsonObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_TABLE_NAME)).append("_");
		key.append("s").append(jsonObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_SEQNO)).append("_");
		key.append("v").append(jsonObj.getString(DataPullConstants.FullPullInterfaceJson.VERSION_KEY));
		return key.toString();
	}

	private static String makePendingTaskKey(String dataSourceInfo) {
		assert (dataSourceInfo != null);

		// 生成用于记录pending task的key。
		JSONObject dsObj = JSONObject.parseObject(dataSourceInfo).getJSONObject(DataPullConstants.FullPullInterfaceJson.PAYLOAD_KEY);
		/*
		 * 整个程序流转dataSourceInfo的全息信息很重要，所以利用dataSourceInfo来生成key。
		 * 但dataSourceInfo在处理过程中发生了变化，多了VERSION field（目前变化的部分只有VERSION）。
		 * 为了保证同一个pending task的监控key一致，以便ADD和REMOVE操作能对应上，需要剔除变化的部分。
		 * 这样做看起来不是特别好，但没有找到更好的方法。
		 */
		dsObj.remove("VERSION");

		// 双引号在存储到zk时，会带转义符。为了避免其带来的负面影响，key剔除所有双引号，以便于比较。
		// 这个方法中没有用 JSONArray，然后直接利用其
		// contains，add，remove等方法处理pending任务的记录和移出，原因也在于转义符导致key对不上。
		// 原始的dataSourceInfo还需用来进行resume
		// message的发送等，需保留其原样，所以放弃JSONArray这种数据结构,后续采用JSONObject put
		// key-value对的方式来处理。
		String pendingTaskKey = dsObj.toString().replace("\"", "");
		return pendingTaskKey;
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
			if (DataPullConstants.FULLPULL_PENDING_TASKS_OP_PULL_TOPOLOGY_RESTART.equals(opType)) {
				List<TopicPartition> list = Arrays.asList(new TopicPartition(topic, 0));
				consumer.seekToEnd(list);
				LOG.info("pullingSpout restart :pulling consumer seek to end !");
			} else if (DataPullConstants.FULLPULL_PENDING_TASKS_OP_SPLIT_TOPOLOGY_RESTART.equals(opType)) {
				conn = DBHelper.getDBusMgrConnection();
				StringBuilder sql = new StringBuilder();
				sql.append("SELECT * FROM   ");
				sql.append("( SELECT h.* FROM t_fullpull_history h   ");
				sql.append("WHERE h.type = '").append(type).append("'");
				if (!isGlobal) {
					sql.append(" and h.dsName = '").append(dsName).append("'");
				}
				sql.append(") h1 ");
				sql.append("WHERE h1.init_time >   ");
				sql.append("( SELECT h2.init_time FROM   ");
				sql.append("( SELECT h.* FROM t_fullpull_history h   ");
				sql.append("WHERE h.type = '").append(type).append("'");
				if (!isGlobal) {
					sql.append(" and h.dsName = '").append(dsName).append("'");
				}
				sql.append(") h2 ");
				sql.append("WHERE h2.state = 'ending' OR h2.state = 'abort' ORDER BY h2.init_time DESC LIMIT 1 )   ");
				sql.append("ORDER BY h1.init_time LIMIT 2  ");

				LOG.info("splittingSpout restart :{}", sql.toString());
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
						fullPullReqMsgOffset1 = rs.getLong(DataPullConstants.FullPullHistory.FULL_PULL_REQ_MSG_OFFSET);
					}
					if (rs.getRow() == 2) {
						fullPullReqMsgOffset2 = rs.getLong(DataPullConstants.FullPullHistory.FULL_PULL_REQ_MSG_OFFSET);
					}
				}

				//存在pending任务
				if (pendingFlag) {
					if (fullPullReqMsgOffset2 != null) {
						TopicPartition topicPartition = new TopicPartition(topic, 0);
						consumer.seek(topicPartition, fullPullReqMsgOffset2);
						LOG.info("splittingSpout restart : seek consumer to the first task and the state is init ! " +
								"FULL_PULL_REQ_MSG_OFFSET :{}", fullPullReqMsgOffset2);
					}

					//pending任务状态置为abort
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

					LOG.info("splittingSpout restart : find pening task ! set the state of pending task to abort! id:{}", rowId1);
				} else {
					if (rowId1 != null) {
						TopicPartition topicPartition = new TopicPartition(topic, 0);
						consumer.seek(topicPartition, fullPullReqMsgOffset1);
						LOG.info("splittingSpout restart : no pening task ! seek consumer to the first task and the state is init !" +
								" FULL_PULL_REQ_MSG_OFFSET :{}!", fullPullReqMsgOffset1);
					} else {
						LOG.info("splittingSpout restart : no pening task ! seek consumer to end !");
					}
				}
				//更新该数据源pending任务的监控节点信息为完成状态
				finishPullReport(dsName);

				if (!isGlobal) {
					//把所有的普通阻断增量的拉全量状态置为abort,是否还会处理这些任务需要看他们的full_pull_req_msg_offset offset的位置决定
					sql = new StringBuilder();
					sql.append(" UPDATE ");
					sql.append("     t_fullpull_history");
					sql.append(" SET state = 'abort', error_msg='全量程序重启,阻断式拉全量状态置为abort,该任务需要重新拉取!' ");
					sql.append(" WHERE type='normal' and dsName=?");
					ps = conn.prepareStatement(sql.toString());
					ps.setString(1, dsName);
					ps.executeUpdate();
				}
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (ps != null)
					ps.close();
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				LOG.error(e.getMessage(), e);
			}
		}
	}


	/**
	 * 第三个参数dataSourceInfo可能传null值，此时操作类型一定是FULLPULL_PENDING_TASKS_OP_CRASHED_NOTIFY。
	 * crash检查是全局的，而不是针对某一个datasource。所以无特定的dataSourceInfo。
	 * crash的处理未用到dataSourceInfo参数，所以这种情况下传null不影响逻辑。
	 */
	public static void updatePendingTasksTrackInfo(ZkService zkService, String dsName,
	                                               String dataSourceInfo, String opType) {
		try {
			zkService = getZkService();

			//1 get pendingNodPath
			String pendingZKNodePath;
			if (isGlobal) {
				pendingZKNodePath = zkMonitorRootNodePath;
			} else {
				if (dataSourceInfo != null) {
					pendingZKNodePath = getCurrentPendingZKNodePath(zkMonitorRootNodePath, dsName, dataSourceInfo);
				} else {
					pendingZKNodePath = zkMonitorRootNodePath + "/" + dsName;
				}
			}
			if (!zkService.isExists(pendingZKNodePath)) {
				LOG.error("Not found zk node of path '{}' when checking pending tasks.", pendingZKNodePath);
				return;
			}

			JSONObject pendingNodeContent = null;
			byte[] pendingNodeContentByte = zkService.getData(pendingZKNodePath);
			if (pendingNodeContentByte != null && pendingNodeContentByte.length > 0) {
				pendingNodeContent = JSONObject.parseObject(new String(pendingNodeContentByte));
			} else {
				pendingNodeContent = new JSONObject();
			}


			// 2 get pendingTask
			// 引入topologyId做key，以免pulling和splitting的消息相互交叉覆盖，作区分
			String topologyId = getTopologyId();
			JSONObject pendingTasksJsonObj;
			String pendingTasksJsonObjKey = DataPullConstants.FULLPULL_PENDING_TASKS + topologyId;
			if (pendingNodeContent.containsKey(pendingTasksJsonObjKey)) {
				pendingTasksJsonObj = pendingNodeContent.getJSONObject(pendingTasksJsonObjKey);
			} else {
				pendingTasksJsonObj = new JSONObject();
			}


			if (DataPullConstants.FULLPULL_PENDING_TASKS_OP_ADD_WATCHING.equals(opType)) {
				String pendingTaskKey = makePendingTaskKey(dataSourceInfo);
				if (!pendingTasksJsonObj.containsKey(pendingTaskKey)) {
					pendingTasksJsonObj.put(pendingTaskKey, dataSourceInfo);
					pendingNodeContent.put(pendingTasksJsonObjKey, pendingTasksJsonObj);
					LOG.info("Fullpull task [{}] come from {} is added to watching list!", dataSourceInfo, topologyId);
				} else {
					//not suppose to here
					LOG.warn("duplicate task [{}] come from {} i existed in watching list!", dataSourceInfo, topologyId);
				}

			} else if (DataPullConstants.FULLPULL_PENDING_TASKS_OP_REMOVE_WATCHING.equals(opType)) {
				String pendingTaskKey = makePendingTaskKey(dataSourceInfo);
				pendingTasksJsonObj.remove(pendingTaskKey);
				pendingNodeContent.put(pendingTasksJsonObjKey, pendingTasksJsonObj);

				LOG.info("Fullpull task [{}] is removing from watching list by {}!", dataSourceInfo, topologyId);

			} else if (DataPullConstants.FULLPULL_PENDING_TASKS_OP_CRASHED_NOTIFY.equals(opType)) {
				// 检查是否有遗留未决的拉取任务。如果有，需要resolve（发resume消息通知给appender，并在zk上记录，且将监测到的pending任务写日志，方便排查问题）。
				if (pendingTasksJsonObj != null && !pendingTasksJsonObj.isEmpty()) {
					for (Map.Entry<String, Object> entry : pendingTasksJsonObj.entrySet()) {
						String pendingDataSourceInfo = (String) entry.getValue();

						//构建错误msg等
						JSONObject dataSourceInfoJsonObj = JSONObject.parseObject(pendingDataSourceInfo);
						String schema = dataSourceInfoJsonObj
								.getJSONObject(DataPullConstants.FullPullInterfaceJson.PAYLOAD_KEY)
								.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_SCHEMA_NAME);
						String table = dataSourceInfoJsonObj
								.getJSONObject(DataPullConstants.FullPullInterfaceJson.PAYLOAD_KEY)
								.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_TABLE_NAME);
						String schemaAndTableInfo = String.format("%s.%s", schema, table);

						String errorMsg = String.format("Pending fullpull task for [%s] is found by %s.",
								schemaAndTableInfo, topologyId);

						//发送abort
						finishPullReport(zkService, pendingDataSourceInfo,
								FullPullHelper.getCurrentTimeStampString(), Constants.DataTableStatus.DATA_STATUS_ABORT,
								errorMsg);

						errorMsg = String.format("Pending fullpull task for [%s] found by %s is resolved.",
								schemaAndTableInfo, topologyId);
						LOG.info(errorMsg);
					}

					// 对于已经resolve的pending任务，将其移出pending队列，以免造成无限重复处理。
					pendingNodeContent.put(pendingTasksJsonObjKey, new JSONObject());// 清空pending队列
				} else {
					LOG.info("OK! no pending job.");
				}
			}
			zkService = getZkService();
			try {
				zkService.setData(pendingZKNodePath, pendingNodeContent.toString().getBytes());
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
			}

			//todo 更新数据库，写拉全量状态

			LOG.info("Pending Tasks Watching Node on zk is updated!");
		} catch (Exception e) {
			LOG.error("Maintaining pending tasks encounter error!", e);
		}
	}

	public static String getCurrentPendingZKNodePath(String monitorRoot, String dsName, String dataSourceInfo) {
		JSONObject reqJson = JSONObject.parseObject(dataSourceInfo);
		JSONObject projectJson = reqJson.getJSONObject("project");
		String pendingZKNodePath;
		if (projectJson != null && !projectJson.isEmpty()) {
			String projectName = projectJson.getString("name");
			String projectId = projectJson.getString("id");
			pendingZKNodePath = Constants.FULL_PULL_PROJECTS_MONITOR_ROOT + "/" + projectName + "_" + projectId + "/" + dsName;
		} else {
			pendingZKNodePath = monitorRoot + "/" + dsName;
		}
		return pendingZKNodePath;
	}

	public static Map loadConfProps(String zkconnect, String topid, String zkTopoRoot, String consumerSubTopicKey) {
		Map confMap = new HashMap<>();
		try {
			//连接zk
			FullPullPropertiesHolder.initialize(getTopologyType(), zkconnect, zkTopoRoot);
			initialize(topid, zkconnect);
			//从zk中获取common-config节点的内容
			Properties commonProps = getFullPullProperties(Constants.ZkTopoConfForFullPull.COMMON_CONFIG, true);
			//创建zk连接，将其存入confMap中
			ZkService zkService = getZkService();
			//最大流门限值
			Integer maxFlowThreshold = Integer.valueOf(commonProps.getProperty(Constants.ZkTopoConfForFullPull.SPOUT_MAX_FLOW_THRESHOLD));
			LOG.info("maxFlowThreshold is {} on FullPullHelper.loadConfProps", maxFlowThreshold);
			//加入confMap
			confMap.put(RUNNING_CONF_KEY_COMMON, commonProps);
			confMap.put(RUNNING_CONF_KEY_MAX_FLOW_THRESHOLD, maxFlowThreshold);
			confMap.put(RUNNING_CONF_KEY_ZK_SERVICE, zkService);

			if (StringUtils.isNotBlank(consumerSubTopicKey)) {
				String consumerSubTopic = commonProps.getProperty(consumerSubTopicKey);
				//查询数据库处理CtrlTopic
				if (Constants.ZkTopoConfForFullPull.FULL_PULL_SRC_TOPIC.equalsIgnoreCase(consumerSubTopicKey)) {
					consumerSubTopic = getCtrlTopicFromDB(topid, consumerSubTopic);
				}
				Properties consumerProps = getFullPullProperties(Constants.ZkTopoConfForFullPull.CONSUMER_CONFIG, true);
				Consumer<String, byte[]> consumer = DbusHelper.createConsumer(consumerProps, consumerSubTopic);
				confMap.put(RUNNING_CONF_KEY_CONSUMER, consumer);
			}

			Properties byteProducerProps = FullPullHelper.getFullPullProperties(Constants.ZkTopoConfForFullPull.BYTE_PRODUCER_CONFIG, true);
			if (null != byteProducerProps) {
				Producer byteProducer = DbusHelper.getProducer(byteProducerProps);
				confMap.put(RUNNING_CONF_KEY_BYTE_PRODUCER, byteProducer);
			}

			Properties stringProducerProps = FullPullHelper.getFullPullProperties(Constants.ZkTopoConfForFullPull.STRING_PRODUCER_CONFIG, true);
			if (null != stringProducerProps) {
				Producer stringProducer = DbusHelper.getProducer(stringProducerProps);
				confMap.put(RUNNING_CONF_KEY_STRING_PRODUCER, stringProducer);
				confMap.put(RUNNING_CONF_KEY_STRING_PRODUCER_PROPS, stringProducerProps);
			}
		} catch (Exception e) {
			LOG.error("Encountered exception:", e);
			throw new InitializationException();
		}
		return confMap;
	}

	private static String getCtrlTopicFromDB(String topid, String consumerSubTopic) {

		String dsName = topid.substring(0, topid.indexOf("-"));
		//testdb-fullsplitter/testdb-fullpuller
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = DBHelper.getDBusMgrConnection();
			StringBuilder sql = new StringBuilder();
			sql.append("SELECT ");
			sql.append("d.ctrl_topic ");
			sql.append("FROM ");
			sql.append("t_dbus_datasource d ");
			sql.append("WHERE ");
			sql.append("d.ds_name = ? ");

			ps = conn.prepareStatement(sql.toString());
			ps.setString(1, dsName);
			rs = ps.executeQuery();
			if (rs.next()) {
				String ctrl_topic = rs.getString("ctrl_topic");
				LOG.info("fullpull.src.topic from database is {}", ctrl_topic);
				return ctrl_topic;
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		} finally {
			DBHelper.close(conn, ps, rs);
		}
		return consumerSubTopic;
	}

//    public static void saveEncodeTypeonZk(ZkService zkService) {
//        //获取内置的加密类型
//        Map<String, String> map = new HashMap<>();
//        for (MessageEncodeType type : MessageEncodeType.values()) {
//            map.put(type.name().toLowerCase(), "Built in encryption");
//        }
//        //检测第三方加密类型
//        Map<String, Class<ExtEncodeStrategy>> extMap = ExternalEncoders.get();
//        for(Map.Entry<String, Class<ExtEncodeStrategy>> entry : extMap.entrySet()) {
//            String key = entry.getKey();
//            String value = entry.getValue().getName();
//            map.put(key,value);
//        }
//        LOG.info("encodeTypes map:" + map);
//
//        String zkPath = "/DBus/encoderType";
//        String updateInfo = map.toString();
//        if(zkService == null) {
//            return;
//        }
//        try {
//            if(!zkService.isExists(zkPath)) {
//                zkService.createNode(zkPath, null);
//            }
//            zkService.setData(zkPath, updateInfo.getBytes());
//        } catch (Exception e) {
//            LOG.error(e.getMessage(),e);
//        }
//    }


	@SuppressWarnings("unchecked")
	public static Map reloadZkServiceConfProps(String zkconnect, String zkTopoRoot) {
		Map confMap = new HashMap<>();
		try {
			Properties commonProps = getFullPullProperties(Constants.ZkTopoConfForFullPull.COMMON_CONFIG, true);
			String zkConString = commonProps.getProperty(Constants.ZkTopoConfForFullPull.FULL_PULL_MONITORING_ZK_CONN_STR);

			ZkService zkService = null;
			try {
				zkService = new ZkService(zkConString);
			} catch (Exception e) {
				LOG.error("Create new zkservice failed. Exception:" + e);
			}
			confMap.put(RUNNING_CONF_KEY_ZK_SERVICE, zkService);
		} catch (Exception e) {
			LOG.error("Encountered exception:", e);
			throw new InitializationException();
		}
		return confMap;
	}


	public static void saveReloadStatus(String json, String title, boolean prepare, String zkServers) {
		if (json != null) {
			String msg = title + " reload successful!";
			ControlMessage message = ControlMessage.parse(json);
			CtlMessageResult result = new CtlMessageResult(title, msg);
			result.setOriginalMessage(message);
			CtlMessageResultSender sender = new CtlMessageResultSender(message.getType(), zkServers);
			sender.send(title, result, prepare, false);
			LOG.info("Write reload status to zk successfully. Msg:{}", msg);
		}
	}

	public static boolean isDbusKeeper(String dataSourceInfo) {
		JSONObject json = JSONObject.parseObject(dataSourceInfo);
		JSONObject project = json.getJSONObject("project");
		if (project != null && !project.isEmpty()) {
			return true;
		}
		return false;
	}

	public static String getTopoName(String dataSourceInfo) {
		JSONObject json = JSONObject.parseObject(dataSourceInfo);
		JSONObject project = json.getJSONObject("project");
		Integer topo_table_id = project.getInteger("topo_table_id");
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		String topo_name = null;
		try {
			conn = DBHelper.getDBusMgrConnection();
			String sql = "select p.topo_name from t_project_topo_table t ,t_project_topo p " +
					"where t.topo_id = p.id and t.id = " + topo_table_id;
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			if (rs.next()) {
				topo_name = rs.getString("topo_name");
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (ps != null)
					ps.close();
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				LOG.error(e.getMessage(), e);
			}
		}
		return topo_name;
	}
}
