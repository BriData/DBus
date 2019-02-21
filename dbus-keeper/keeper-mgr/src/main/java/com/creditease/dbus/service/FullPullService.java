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
import com.creditease.dbus.utils.StormToplogyOpHelper;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.creditease.dbus.constant.KeeperConstants.GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS;


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
			String resultTopic = payload.get("resultTopic").toString();

             /*
             对于mysql
             需要去源端查询physical_table_regex中的内容
             */
			if (StringUtils.equalsIgnoreCase(table.getDsType(), "mysql")) {
				Class.forName("com.mysql.jdbc.Driver");
				Connection conn = DriverManager.getConnection(table.getMasterUrl(), table.getDbusUser(), table.getDbusPassword());
				InitialLoadService ilService = InitialLoadService.getService();
				String physicalTables = ilService.getMysqlTables(conn, table);
				conn.close();
				payload.put("PHYSICAL_TABLES", physicalTables);
			} else if (StringUtils.equalsIgnoreCase(table.getDsType(), "oracle")) {
				Class.forName("oracle.jdbc.driver.OracleDriver");
				Connection conn = DriverManager.getConnection(table.getMasterUrl(), table.getDbusUser(), table.getDbusPassword());
				InitialLoadService ilService = InitialLoadService.getService();
				String physicalTables = ilService.getOracleTables(conn, table);
				conn.close();
				payload.put("PHYSICAL_TABLES", physicalTables);
			}

			payload = this.getOPTS(table.getOutputTopic(), payload);
			controlMessage.setPayload(payload);

			/*String key;
			String value;

			Properties consumerProps = zkService.getProperties(KeeperConstants.KEEPER_CONSUMER_CONF);
			consumerProps.setProperty("client.id", "full-pull.reader");
			consumerProps.setProperty("group.id", "full-pull.reader");
			Properties globalConf = zkService.getProperties(KeeperConstants.GLOBAL_CONF);
			consumerProps.setProperty(GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS, globalConf.getProperty(GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS));

			consumer = new KafkaConsumer(consumerProps);
			logger.info("[control message] Send control message to ctrlTopic: {} \n resultTopic: {} \n ", ctrlTopic, resultTopic);

			TopicPartition dataTopicPartition = new TopicPartition(table.getOutputTopic(), 0);
			List<TopicPartition> topics = Arrays.asList(dataTopicPartition);
			consumer.assign(topics);
			consumer.seekToBeginning(topics);
			long beginOffset = consumer.position(dataTopicPartition);
			consumer.seekToEnd(topics);
			long offset = consumer.position(dataTopicPartition);
			final String OP_TS = "OP_TS";
			long step = Integer.valueOf(consumerProps.getProperty("max.poll.records"));
			boolean runing = controlMessage.getPayload().get("POS") == null || controlMessage.getPayload().get(OP_TS) == null;
			while (runing) {
				ConsumerRecords<String, byte[]> results = consumer.poll(100);
				while (results.isEmpty()) {
					offset = offset - step;
					if (offset < beginOffset) {
						logger.info("没有找到 op_ts .");
						payload.put("POS", 0);
						payload.put(OP_TS, DateFormatUtils.format(Calendar.getInstance(), "yyyy-MM-dd HH:mm:ss.SSS"));
						controlMessage.setPayload(payload);
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
						String pos = jsonDbusMessage.getJSONArray("payload").getJSONObject(0).getJSONArray("tuple").getString(iPos);
						String op_ts = jsonDbusMessage.getJSONArray("payload").getJSONObject(0).getJSONArray("tuple").getString(iOpTs);
						logger.info("找到了op_ts, pos : {} ,op_ts : {}", pos, op_ts);
						payload.put("POS", pos);
						payload.put(OP_TS, op_ts);
						controlMessage.setPayload(payload);
						runing = false;
						break;
					}
				}
			}*/

			//解决不同终端机器时间不一致导致的t_fullpull_history表的id排序和full_pull_req_msg_offset排序不一致的问题
			Date date = new Date();
			long time = date.getTime();
			String timestamp = sdf.format(date);
			payload.put("SEQNO", time);
			controlMessage.setPayload(payload);
			controlMessage.setId(time);
			controlMessage.setTimestamp(timestamp);
			fullPullHistory.setId(time);

			ControlMessageSender sender = ControlMessageSenderProvider.getControlMessageSender(zkService);

			requestSender.post(MS_SERVICE, "/fullPullHistory/create", fullPullHistory);

			long positionOffset = sender.send(ctrlTopic, controlMessage);
			fullPullHistory.setFullPullReqMsgOffset(positionOffset);
			fullPullHistory.setTargetSinkTopic(resultTopic);
			requestSender.post(MS_SERVICE, "/fullPullHistory/update", fullPullHistory);
			logger.info("[control message] Send control message to ctrlTopic[{}] success.\n message.pos: {} \n message.op_ts: {}",
					ctrlTopic, controlMessage.getPayload().get("POS"), controlMessage.getPayload().get("OP_TS"));
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

	public JSONObject buildPayload(String resultTopic, Long time, DataTable dataTable) {

		if (resultTopic == null) {
			// 默认的构造规则
			resultTopic = dataTable.getDsType() + "." + dataTable.getSchemaName() + "." +
					dataTable.getTableName() + "." + String.valueOf(time);
		}
		JSONObject payload = new JSONObject();

		payload.put("DBUS_DATASOURCE_ID", String.valueOf(dataTable.getDsId()));
		payload.put("SCHEMA_NAME", dataTable.getSchemaName());
		payload.put("TABLE_NAME", dataTable.getTableName());
		payload.put("INCREASE_VERSION", "false");
		payload.put("INCREASE_BATCH_NO", "false");
		payload.put("resultTopic", resultTopic);
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
	                          String splitCol, String splitShardStyle, String inputConditions) throws Exception {
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
			message = createProjectFullPullMessage(topoTable, dataTable, outputTopic);
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
			message = buildSourceFullPullMessage(dataTable, outputTopic);
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
		if (!StormToplogyOpHelper.inited) {
			StormToplogyOpHelper.init(zkService);
		}
		if (StormToplogyOpHelper.getTopologyByName(dsName + "-splitter-puller") == null) {
			return MessageCode.FULLPULL_TOPO_IS_NOT_RUNNING;
		}
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
			logger.warn("[globalfullPull] fullPullEnable of resource is false . projectResourceId:{}，topoTableId:{}", resource.getId(), topoTableId);
			return MessageCode.TABLE_RESOURCE_FULL_PULL_FALSE;
		} else {
			return 0;
		}
	}

	public JSONObject buildSourceFullPullMessage(DataTable dataTable, String outputTopic) {
		Date date = new Date();
		JSONObject message = buildMessage(date);
		JSONObject payloadJson = buildPayload(outputTopic, date.getTime(), dataTable);
		message.put("payload", payloadJson);
		return message;
	}

	public JSONObject createProjectFullPullMessage(ProjectTopoTable topoTable, DataTable dataTable, String outputTopic) throws Exception {
		Project project = projectService.queryProjectId(topoTable.getProjectId()).getPayload(Project.class);

		Date date = new Date();
		JSONObject message = this.buildMessage(date);
		JSONObject payloadJson = this.buildPayload(outputTopic, date.getTime(), dataTable);
		JSONObject projectJson = this.buildProject(topoTable, project.getProjectName());
		message.put("payload", payloadJson);
		message.put("project", projectJson);
		return message;
	}

	public Map<String, Object> getOPTS(String outputTopic, Map<String, Object> payload) throws Exception {
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
		consumerProps.setProperty(GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS, globalConf.getProperty(GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS));


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
						String pos = jsonDbusMessage.getJSONArray("payload").getJSONObject(0).getJSONArray("tuple").getString(iPos);
						String op_ts = jsonDbusMessage.getJSONArray("payload").getJSONObject(0).getJSONArray("tuple").getString(iOpTs);
						logger.info("找到了op_ts, pos : {} ,op_ts : {}", pos, op_ts);
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

}
