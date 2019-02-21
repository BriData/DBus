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
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.constant.ServiceNames;
import com.creditease.dbus.domain.model.DataSource;
import com.creditease.dbus.domain.model.DataTable;
import com.creditease.dbus.domain.model.FullPullHistory;
import com.creditease.dbus.domain.model.ProjectTopoTable;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.utils.*;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.creditease.dbus.constant.KeeperConstants.*;

/**
 * Created by xiancangao on 2018/05/04.
 */
@Service
public class ToolSetService {

	@Autowired
	private IZkService zkService;

	@Autowired
	private ZkConfService zkConfService;

	@Autowired
	private FullPullService fullPullService;

	@Autowired
	private Environment env;

	@Autowired
	private RequestSender sender;

	@Autowired
	private ConfigCenterService configCenterService;

	private Logger logger = LoggerFactory.getLogger(getClass());

	public static final String LOG_PROCESSOR_RELOAD_CONFIG = "LOG_PROCESSOR_RELOAD_CONFIG";
	public static final String EXTRACTOR_RELOAD_CONF = "EXTRACTOR_RELOAD_CONF";
	public static final String DISPATCHER_RELOAD_CONFIG = "DISPATCHER_RELOAD_CONFIG";
	public static final String APPENDER_RELOAD_CONFIG = "APPENDER_RELOAD_CONFIG";
	public static final String FULL_DATA_PULL_RELOAD_CONF = "FULL_DATA_PULL_RELOAD_CONF";
	public static final String HEARTBEAT_RELOAD_CONFIG = "HEARTBEAT_RELOAD_CONFIG";

	private static Map<Integer, DataSource> dataSourceMap = new HashMap<>();
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	public Integer sendCtrlMessage(Map<String, String> map) {
		if (!validate(map)) {
			return MessageCode.PARAM_IS_WRONG;
		}
		String strMessage = map.get("message");
		ControlMessage message = ControlMessage.parse(strMessage);
		String type = message.getType();
		//reload心跳
		if (HEARTBEAT_RELOAD_CONFIG.equals(type)) {
			reloadHeartBeat();
			logger.info("reload heartBeat request process ok..");
		} else {
			ControlMessageSender sender = ControlMessageSenderProvider.getControlMessageSender(zkService);
			if (!validate(message)) {
				return MessageCode.MESSAGE_IS_WRONG;
			}
			String topic = map.get("topic");
			try {
				sender.send(topic, message);
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				return MessageCode.EXCEPTION_ON_SEND_MESSAGE;
			}
			logger.info("Send control message request process ok.");
		}
		return 0;
	}

	public JSONObject readZKNode(String type) throws Exception {
		switch (type) {
			case LOG_PROCESSOR_RELOAD_CONFIG:
				return getZkNodes(Constants.CONTROL_MESSAGE_RESULT_ROOT + "/" + LOG_PROCESSOR_RELOAD_CONFIG);
			case EXTRACTOR_RELOAD_CONF:
				return getZkNodes(Constants.CONTROL_MESSAGE_RESULT_ROOT + "/" + EXTRACTOR_RELOAD_CONF);
			case DISPATCHER_RELOAD_CONFIG:
				return getZkNodes(Constants.CONTROL_MESSAGE_RESULT_ROOT + "/" + DISPATCHER_RELOAD_CONFIG);
			case APPENDER_RELOAD_CONFIG:
				return getZkNodes(Constants.CONTROL_MESSAGE_RESULT_ROOT + "/" + APPENDER_RELOAD_CONFIG);
			case FULL_DATA_PULL_RELOAD_CONF:
				return getZkNodes(Constants.CONTROL_MESSAGE_RESULT_ROOT + "/" + FULL_DATA_PULL_RELOAD_CONF);
			case HEARTBEAT_RELOAD_CONFIG:
				return getZkNodes(Constants.CONTROL_MESSAGE_RESULT_ROOT + "/HEARTBEAT_RELOAD");
		}
		return null;
	}

	private JSONObject getZkNodes(String path) throws Exception {
		JSONObject json = null;
		if (zkService.isExists(path)) {
			byte[] data = zkService.getData(path);
			json = JSONObject.parseObject(new String(data, UTF8));
			List<String> children = zkService.getChildren(path);
			if (children.size() > 0) {
				for (String childPath : children) {
					byte[] childData = zkService.getData(path + "/" + childPath);
					JSONObject childJson = JSONObject.parseObject(new String(childData, UTF8));
					json.put(childPath, childJson);
				}
			}
		}
		return json;
	}

	public Integer globalFullPull(Map<String, String> map) throws Exception {
		String strMessage = null;
		strMessage = map.get("message");
		ControlMessage message = ControlMessage.parse(strMessage);
		String id = map.get("id");
		String dsName = map.get("dsName");
		String schemaName = map.get("schemaName");
		String tableName = map.get("tableName");
		DataTable dataTable = this.findTable(dsName, schemaName, tableName);

		//没有topo不允许拉全量
		if (!StormToplogyOpHelper.inited) {
			StormToplogyOpHelper.init(zkService);
		}
		if (StormToplogyOpHelper.getTopologyByName(dsName + "-splitter-puller") == null) {
			return MessageCode.FULLPULL_TOPO_IS_NOT_RUNNING;
		}

		if (dataTable == null) {
			return MessageCode.TABLE_NOT_FOUND_BY_PARAM;
		}
		//判断表类型是否支持拉全量操作
		DbusDatasourceType dsType = DbusDatasourceType.parse(dataTable.getDsType());
		if (DbusDatasourceType.MONGO != dsType &&
				DbusDatasourceType.ORACLE != dsType && DbusDatasourceType.MYSQL != dsType) {
			logger.error("Illegal datasource type:" + dataTable.getDsType());
			ResultEntity resultEntity = new ResultEntity();
			return MessageCode.TYPE_OF_TABLE_CAN_NOT_FULLPULL;
		}

		dataTable.setCtrlTopic(map.get("ctrlTopic"));
		//生成fullPullHistory对象
		FullPullHistory fullPullHistory = new FullPullHistory();
		fullPullHistory.setId(Long.parseLong(id));
		fullPullHistory.setType(map.getOrDefault("type", "global"));
		fullPullHistory.setDsName(dataTable.getDsName());
		fullPullHistory.setSchemaName(dataTable.getSchemaName());
		fullPullHistory.setTableName(dataTable.getTableName());
		fullPullHistory.setState("init");
		fullPullHistory.setInitTime(new Date(fullPullHistory.getId()));
		fullPullHistory.setUpdateTime(fullPullHistory.getInitTime());

		//发送消息
		int sendResult = fullPullService.sendMessage(dataTable, message.toJSONString(), fullPullHistory);
		if (0 != sendResult) {
			logger.info("[send global-full-pull message] exception! ");
			return sendResult;
		}
		return 0;
	}

	private boolean validate(Map<String, String> map) {
		if (map == null) return false;
		for (Map.Entry<String, String> entry : map.entrySet()) {
			if (StringUtils.isBlank(entry.getValue())) {
				return false;
			}
		}
		return true;
	}

	private boolean validate(ControlMessage message) {
		if (message.getId() <= 0) return false;
		if (StringUtils.isBlank(message.getType())) return false;
		if (StringUtils.isBlank(message.getFrom())) return false;
		if (StringUtils.isBlank(message.getTimestamp())) return false;
		return true;
	}

	private DataTable findTable(String dsName, String schemaName, String tableName) {
		String param = "dsName={0}&schemaName={2}&tableName={3}";
		ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/tables/get-by-ds-schema-table-name",
				param, dsName, schemaName, tableName);
		return result.getBody().getPayload(DataTable.class);
	}

	public ResultEntity kafkaReader(Map<String, String> map, Integer userId, String userRole) throws Exception {
		ResultEntity resultEntity = new ResultEntity();
		//TODO
		String bootstrapServers = map.get(GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS);
		String topic = map.get("topic");
		Set<String> topicsByUserId = getTopicsByUserId(bootstrapServers, null, userId, userRole);
		if (!userRole.equals("admin") && !topicsByUserId.contains(topic)) {
			resultEntity.setStatus(MessageCode.NO_AUTHORITY_FOR_THIS_TOPIC);
			resultEntity.setMessage("该用户没有该topic(" + topic + ")的读取的权限!");
			return resultEntity;
		}
		Long from = Long.parseLong(map.get("from"));
		Integer length = Integer.parseInt(map.get("length"));
		String params = map.get("params");
		String[] paramArr = null;
		//过滤参数
		if (StringUtils.isNotBlank(params)) {
			paramArr = params.split(",");
		}

		String negParams = map.get("negParams");
		String[] negParamsArr = null;
		//不包含过滤参数
		if (StringUtils.isNotBlank(negParams)) {
			negParamsArr = negParams.split(",");
		}

		TopicPartition dataTopicPartition = new TopicPartition(topic, 0);
		List<TopicPartition> topics = Arrays.asList(dataTopicPartition);

		Properties consumerProps = zkService.getProperties(KEEPER_CONSUMER_CONF);
		consumerProps.setProperty("client.id", "");
		consumerProps.setProperty("group.id", "dbus-tools.reader");
		Properties globalConf = zkService.getProperties(GLOBAL_CONF);
		consumerProps.setProperty(GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS, globalConf.getProperty(GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS));
		KafkaConsumer<String, String> consumer = null;
		ArrayList<String> result;
		try {
			consumer = new KafkaConsumer(consumerProps);
			consumer.assign(topics);
			consumer.seekToEnd(topics);
			long endOffset = consumer.position(dataTopicPartition);
			consumer.seekToBeginning(topics);
			long beginOffset = consumer.position(dataTopicPartition);

			if (beginOffset > from || endOffset < (from + length)) {
				resultEntity.setStatus(MessageCode.PARAM_IS_WRONG);
				resultEntity.setMessage("传入的起始offset,或者length不正确");
				return resultEntity;
			}
			int readCount = 0;
			boolean running = true;
			result = new ArrayList<>();
			long totalMemSize = 0;
			consumer.seek(dataTopicPartition, from);
			while (running) {
				ConsumerRecords<String, String> records = consumer.poll(1000);
				for (ConsumerRecord<String, String> record : records) {
					String value = record.value();
					if (readCount++ >= length) {
						running = false;
						break;
					}
					//参数过滤
					boolean b = true;
					if (paramArr != null && paramArr.length > 0) {
						for (String param : paramArr) {
							b = b && value.contains(param);
						}
					}
					if (negParamsArr != null && negParamsArr.length > 0) {
						for (String negParam : negParamsArr) {
							b = b && !value.contains(negParam);
						}
					}
					if (b) {
						StringBuilder sb = new StringBuilder();
						sb.append("key:").append(record.key()).append("\n");
						sb.append("offset:").append(record.offset()).append("\n");
						sb.append(value).append("\n\n");
						result.add(sb.toString());
						totalMemSize += sb.toString().getBytes().length;
					}
					//返回值大小控制1001000
					if (totalMemSize > 1001000) {
						running = false;
						break;
					}
				}
				if (consumer.position(dataTopicPartition) == endOffset) {
					break;
				}
			}
			resultEntity.setStatus(ResultEntity.SUCCESS);
			resultEntity.setMessage("ok");
			resultEntity.setPayload(result);
			return resultEntity;
		} finally {
			if (consumer != null) {
				consumer.close();
			}
		}
	}

	public Set<String> getTopics(String bootstrapServers, String param) throws Exception {
		Properties consumerProps = zkService.getProperties(KEEPER_CONSUMER_CONF);
		consumerProps.setProperty("client.id", "");
		consumerProps.setProperty("group.id", "dbus-tools.reader");
		Properties globalConf = zkService.getProperties(GLOBAL_CONF);
		consumerProps.setProperty(GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS, globalConf.getProperty(GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS));
		Map<String, List<PartitionInfo>> stringListMap;
		KafkaConsumer<String, String> consumer = null;
		try {
			consumer = new KafkaConsumer(consumerProps);
			stringListMap = consumer.listTopics();
			Set<String> topics = stringListMap.keySet();
			return topics;
		} finally {
			if (consumer != null) {
				consumer.close();
			}
		}
	}

	public Set<String> getTopicsByUserId(String bootstrapServers, String param, Integer userId, String userRole) throws Exception {
		if (userRole.equals("admin")) {
			return getTopics(bootstrapServers, param);
		}
		List<ProjectTopoTable> tables = sender.get(ServiceNames.KEEPER_SERVICE, "/projectTable/getTopoTablesByUserId/{0}", userId)
				.getBody().getPayload(new TypeReference<List<ProjectTopoTable>>() {
				});
		HashSet<String> topics = new HashSet<>();
		for (ProjectTopoTable table : tables) {
			topics.add(table.getOutputTopic());
		}
		return topics;
	}

	public Map<String, Long> getOffset(String bootstrapServers, String topic) throws Exception {
		TopicPartition dataTopicPartition = new TopicPartition(topic, 0);
		List<TopicPartition> topics = Arrays.asList(dataTopicPartition);

		Properties consumerProps = zkService.getProperties(KEEPER_CONSUMER_CONF);
		consumerProps.setProperty("client.id", "");
		consumerProps.setProperty("group.id", "dbus-tools.reader");
		Properties globalConf = zkService.getProperties(GLOBAL_CONF);
		consumerProps.setProperty(GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS, globalConf.getProperty(GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS));
		KafkaConsumer<String, String> consumer = null;
		try {
			consumer = new KafkaConsumer(consumerProps);
			consumer.assign(topics);
			consumer.seekToBeginning(topics);
			long beginOffset = consumer.position(dataTopicPartition);
			consumer.seekToEnd(topics);
			long endOffset = consumer.position(dataTopicPartition);
			Map<String, Long> map = new HashMap<>();
			map.put("beginOffset", beginOffset);
			map.put("endOffset", endOffset);
			return map;
		} finally {
			if (consumer != null) {
				consumer.close();
			}
		}
	}

	public void sendCtrlMessageEasy(Integer dsId, String dsName, String dsType) {
		if ("mysql".equals(dsType)) {
			reloadConfig(dsId, dsName, APPENDER_RELOAD_CONFIG);
			this.reloadHeartBeat();
			reloadConfig(dsId, dsName, EXTRACTOR_RELOAD_CONF);
		}
		else {
			reloadConfig(dsId, dsName, APPENDER_RELOAD_CONFIG);
			this.reloadHeartBeat();
		}
	}

	/**
	 * 不reload心跳
	 */
	public void reloadMongoCatch(Integer dsId, String dsName, String dsType) {
		if (DbusDatasourceType.MONGO == DbusDatasourceType.parse(dsType)) {
			reloadConfig(dsId, dsName, APPENDER_RELOAD_CONFIG);
			reloadConfig(dsId, dsName, EXTRACTOR_RELOAD_CONF);
		}
	}

	public int reloadConfig(Integer dsId, String dsName, String reloadType) {
		DataSource dataSource = getDataSourceById(dsId);
		HashMap<String, String> map = makeCtrlMessageParam(dataSource.getCtrlTopic());
		JSONObject message = this.bulidMessage(dataSource, reloadType);
		map.put("message", message.toString());
		return this.sendCtrlMessage(map);
	}

	private HashMap<String, String> makeCtrlMessageParam(String ctrlTopic) {
		HashMap<String, String> map = new HashMap<>();
		map.put("topic", ctrlTopic);
		return map;
	}

	private JSONObject bulidMessage(DataSource dataSource, String reloadType) {
		JSONObject payload = new JSONObject();
		payload.put("dsName", dataSource.getDsName());
		payload.put("dsType", dataSource.getDsType());

		JSONObject json = new JSONObject();
		json.put("from", "dbus-web");
		json.put("id", System.currentTimeMillis());
		json.put("payload", payload);
		json.put("timestamp", sdf.format(new Date()));
		json.put("type", reloadType);
		return json;
	}

	/**
	 * reload心跳
	 */
	private void reloadHeartBeat() {
		JSONObject json = new JSONObject();
		json.put("cmdType", "1");
		json.put("args", System.currentTimeMillis());
		try {
			zkService.setData("/DBus/HeartBeat/Control", json.toString().getBytes(UTF8));
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	public ResultEntity sourceTableColumn(Integer tableId, Integer number) throws Exception {
		ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/toolSet/source-table-column/{0}/{1}",
				tableId, number);
		return result.getBody();
	}

	public JSONObject checkEnvironment() throws Exception {
		JSONObject result = new JSONObject();

		Properties global = zkService.getProperties(Constants.GLOBAL_PROPERTIES_ROOT);
		String stormNimbusHost = global.getProperty(GLOBAL_CONF_KEY_STORM_NIMBUS_HOST);
		int stormNimbusPort = Integer.parseInt(global.getProperty(GLOBAL_CONF_KEY_STORM_NIMBUS_PORT));
		String user = global.getProperty(GLOBAL_CONF_KEY_STORM_SSH_USER);

		String pubKeyPath = env.getProperty("pubKeyPath");
		//测试storm免密配置是否可用
		if (StringUtils.isNotBlank(SSHUtils.executeCommand(user, stormNimbusHost, stormNimbusPort, pubKeyPath, "ls", true))) {
			result.put("stormSSHSecretFree", "error");
		} else {
			result.put("stormSSHSecretFree", "ok");
		}
		//storm信息
		if (!StormToplogyOpHelper.inited) {
			StormToplogyOpHelper.init(zkService);
		}
		JSONArray nimbuses = JSONObject.parseObject(StormToplogyOpHelper.nimbusSummary()).getJSONArray("nimbuses");
		JSONArray supervisors = JSONObject.parseObject(StormToplogyOpHelper.supervisorSummary()).getJSONArray("supervisors");
		result.put("nimbuses", nimbuses);
		result.put("supervisors", supervisors);

		//心跳节点状态
		String[] heartbeatList = global.getProperty("heartbeat.host").split(",");
		int heartbeatport = Integer.parseInt(global.getProperty("heartbeat.port"));
		String heartbeatuser = global.getProperty("heartbeat.user");
		ArrayList<Map<String, String>> heartBeatLeader = new ArrayList<>();
		for (String host : heartbeatList) {
			//ps -ef | grep 'com.creditease.dbus.heartbeat.start.Start' | grep -v grep | awk '{print $2}'
			//jps -l | grep 'com.creditease.dbus.heartbeat.start.Start' | awk '{print $1}'
			String pid = SSHUtils.executeCommand(heartbeatuser, host, heartbeatport, pubKeyPath,
					"ps -ef | grep 'com.creditease.dbus.heartbeat.start.Start' | grep -v grep | awk '{print $2}'", false);
			HashMap<String, String> heartbeatStat = new HashMap<>();
			if (StringUtils.isNotBlank(pid)) {
				heartbeatStat.put("host", host);
				heartbeatStat.put("pid", pid);
				heartbeatStat.put("state", "ok");
			} else {
				heartbeatStat.put("host", host);
				heartbeatStat.put("pid", pid);
				heartbeatStat.put("state", "error");
			}
			heartBeatLeader.add(heartbeatStat);
		}
		result.put("heartBeatLeader", heartBeatLeader);
		//kafka节点状态
		String[] bootstrapList = global.getProperty(GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS).split(",");
		ArrayList<Map<String, String>> kafkaBrokers = new ArrayList<>();
		for (String bootstrap : bootstrapList) {
			Map<String, String> kafkaStat = new HashMap<>();
			String[] ipPort = bootstrap.split(":");
			boolean b = configCenterService.urlTest(ipPort[0], Integer.parseInt(ipPort[1]));
			if (b) {
				kafkaStat.put("host", ipPort[0]);
				kafkaStat.put("port", ipPort[1]);
				kafkaStat.put("state", "ok");
			} else {
				kafkaStat.put("host", ipPort[0]);
				kafkaStat.put("port", ipPort[1]);
				kafkaStat.put("state", "error");
			}
			kafkaBrokers.add(kafkaStat);
		}
		result.put("kafkaBrokers", kafkaBrokers);
		//zk节点状态
		String zk_url = global.getProperty("zk.url");
		if (StringUtils.isBlank(zk_url)) {
			zk_url = env.getProperty("zk.str");
		}

		String[] zkServerList = zk_url.split(",");
		ArrayList<Map<String, String>> zkStats = new ArrayList<>();
		for (String zkObj : zkServerList) {
			Map<String, String> zkStat = new HashMap<>();
			String[] monitorZKServer = zkObj.split(":");
			String ruok = this.executeCmd("ruok", monitorZKServer[0], monitorZKServer[1]);
			if ("imok".equals(ruok)) {
				zkStat.put("host", monitorZKServer[0]);
				zkStat.put("port", monitorZKServer[1]);
				zkStat.put("state", "ok");
			} else {
				zkStat.put("host", monitorZKServer[0]);
				zkStat.put("port", monitorZKServer[1]);
				zkStat.put("state", "error");
			}
			zkStats.add(zkStat);
		}
		result.put("zkStats", zkStats);
		//grafana状态
		String grafanaUrl = global.getProperty(GLOBAL_CONF_KEY_GRAFANA_URL_DBUS);
		if (configCenterService.urlTest(grafanaUrl)) {
			result.put("grafanaUrl", "ok");
		} else {
			result.put("grafanaUrl", "error");
		}
		//influxdb状态
		String influxdbUrl = global.getProperty(GLOBAL_CONF_KEY_INFLUXDB_URL_DBUS);
		String url = influxdbUrl + "/query?q=show+databases" + "&db=_internal";
		if ("200".equals(HttpClientUtils.httpGet(url))) {
			result.put("influxdbUrl", "ok");
		} else {
			result.put("influxdbUrl", "error");
		}
		return result;
	}

	private String executeCmd(String cmd, String zkServer, String zkPort) {
		StringBuilder sb;
		try (Socket s = new Socket(zkServer, Integer.parseInt(zkPort));
		     PrintWriter out = new PrintWriter(s.getOutputStream(), true);
		     BufferedReader reader = new BufferedReader(new InputStreamReader(s.getInputStream()))) {
			out.println(cmd);
			String line = reader.readLine();
			sb = new StringBuilder();
			while (line != null) {
				sb.append(line);
				sb.append("\n");
				line = reader.readLine();
			}
			return sb.substring(0, sb.length() - 1).toString();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			return "imnotok";
		}
	}

	public void initConfig(String dsName) throws Exception {
		Properties globalPro = zkService.getProperties(Constants.GLOBAL_PROPERTIES_ROOT);
		try {
			// 如果通过参数传入了数据线名称，直接为具体数据线上载zk 配置。
			if (StringUtils.isNotBlank(dsName)) {
				this.initializeDsZkConf(dsName, globalPro);
			} else {
				// 未传入任何参数，初始化zk节点和数据
				this.initializeZooKeeper(globalPro);
				// 初始化zk节点 mysql.properties配置
				updateMySqlConfig();
			}
			logger.info("Successfully added new nodes on ZooKeeper. ");
		} catch (Exception e) {
			logger.error("Exception caught when initializing Zookeeper!", e);
			throw e;
		}
	}

	/**
	 * 更新zk mysql配置节点信息
	 *
	 * @throws Exception
	 */
	private void updateMySqlConfig() throws Exception {
		HashMap<String, String> map = sender.get(ServiceNames.KEEPER_SERVICE, "/toolSet/getMgrDBMsg").getBody()
				.getPayload(new TypeReference<HashMap<String, String>>() {
				});
		byte[] data = zkService.getData(Constants.MYSQL_PROPERTIES_ROOT);
		LinkedHashMap<String, String> linkedHashMap = formatPropertiesString(new String(data, UTF8));
		linkedHashMap.put("driverClassName", map.get("driverClassName"));
		linkedHashMap.put("url", map.get("url"));
		linkedHashMap.put("username", map.get("username"));
		linkedHashMap.put("password", map.get("password"));
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, String> entry : linkedHashMap.entrySet()) {
			sb.append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
		}
		zkService.setData(Constants.MYSQL_PROPERTIES_ROOT, sb.toString().getBytes(UTF8));
	}

	private void initializeDsZkConf(String dsName, Properties globalPro) throws Exception {
		insertNodeWithCheckDup(Constants.TOPOLOGY_ROOT, null);
		for (String confFilePath : InitZooKeeperNodesTemplate.ZK_TEMPLATES_NODES_PATHS) {
			// 拟添加节点的各级父节点需先生成好
			String[] confFilePathSplitted = confFilePath.split("/");
			int confFilePathPrefixsCount = confFilePathSplitted.length - 1;
			String precedantPath = Constants.DBUS_ROOT;
			for (int i = 1; i < confFilePathPrefixsCount; i++) {
				precedantPath = precedantPath + "/" + confFilePathSplitted[i];
				precedantPath = precedantPath.replace(DS_NAME_PLACEHOLDER, dsName);
				insertNodeWithCheckDup(precedantPath, new byte[0]);
			}

			// 将配置模板上传到zk
			String zkPath = precedantPath + "/" + confFilePathSplitted[confFilePathSplitted.length - 1];
			zkPath = zkPath.replace(DS_NAME_PLACEHOLDER, dsName);
			String fileName = confFilePath;
			byte[] data = ConfUtils.toByteArray(fileName);
			String fileContent = new String(data);
			fileContent = fileContent.replace(BOOTSTRAP_SERVER_PLACEHOLDER, globalPro.getProperty(GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS));
			fileContent = fileContent.replace(ZK_SERVER_PLACEHOLDER, env.getProperty("zk.str"));
			fileContent = fileContent.replace(DS_NAME_PLACEHOLDER, dsName);
			data = fileContent.getBytes();
			insertNodeWithCheckDup(zkPath, data);
		}
		logger.info(String.format("Upload properites success!"));
	}

	/**
	 * 初始化模板配置节点和数据
	 *
	 * @throws Exception
	 */
	private void initializeZooKeeper(Properties globalPro) throws Exception {
		//创建DBus节点
		insertNodeWithCheckDup(Constants.DBUS_ROOT, null);
		//初始化节点和节点数据
		this.initNodeData(InitZooKeeperNodesTemplate.ZK_TEMPLATES_NODES_PATHS, globalPro);
		this.initNodeData(InitZooKeeperNodesTemplate.ZK_OTHER_NODES_PATHS, globalPro);
		//创建其他空节点
		this.initEmptyNode();
	}

	/**
	 * 初始化空节点
	 *
	 * @throws Exception
	 */
	private void initEmptyNode() throws Exception {
		for (String confFilePath : InitZooKeeperNodesTemplate.ZK_EMPTY_NODES_PATHS) {
			insertNodeWithCheckDup(Constants.DBUS_ROOT + "/" + confFilePath, null);
		}
	}

	/**
	 * Insert a node with checking duplicates. If
	 * there is already a node there, we do not insert
	 * in order to avoid errors.
	 */
	private void insertNodeWithCheckDup(String path, byte[] data) throws Exception {
		try {
			if (!zkService.isExists(path)) {
				zkService.createNode(path, data);
				logger.info(String.format("create node '%s' OK!", path));
			} else {
				logger.warn(String.format("Node %s already exists. ", path));
			}
		} catch (Exception e) {
			logger.error("Exception caught when creating a node %s", path, e);
			throw e;
		}
	}

	/**
	 * 初始化节点和节点数据
	 *
	 * @throws Exception
	 */
	private void initNodeData(String[] nodes, Properties globalPro) throws Exception {
		for (String confFilePath : nodes) {
			// 拟添加节点的各级父节点需先生成好
			String[] confFilePathSplitted = confFilePath.split("/");
			int confFilePathPrefixsCount = confFilePathSplitted.length - 1;
			String precedantPath = Constants.DBUS_ROOT;
			for (int i = 0; i < confFilePathPrefixsCount; i++) {
				precedantPath = precedantPath + "/" + confFilePathSplitted[i];
				insertNodeWithCheckDup(precedantPath, null);
			}

			// 将配置模板上传到zk
			String zkPath = Constants.DBUS_ROOT + "/" + confFilePath;
			String fileName = confFilePath;
			byte[] data = ConfUtils.toByteArray(fileName);
			String fileContent = new String(data);
			fileContent = fileContent.replace(BOOTSTRAP_SERVER_PLACEHOLDER, globalPro.getProperty(GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS));
			fileContent = fileContent.replace(ZK_SERVER_PLACEHOLDER, env.getProperty("zk.str"));
			data = fileContent.getBytes();
			insertNodeWithCheckDup(zkPath, data);
		}
		logger.info(String.format("Upload properites success!"));
	}

	/**
	 * 格式化 properties字符串为LinkedHashMap,保留先后顺序
	 *
	 * @param pros
	 * @return
	 */
	public LinkedHashMap<String, String> formatPropertiesString(String pros) {
		LinkedHashMap<String, String> map = new LinkedHashMap<>();
		String[] split = pros.split("\n");
		for (String ss : split) {
			String[] pro = ss.split("=", 2);
			if (pro.length == 2) {
				map.put(pro[0], pro[1]);
			}
		}
		return map;
	}

	public String getGlobalFullPullTopo() throws Exception {
		if (!StormToplogyOpHelper.inited) {
			StormToplogyOpHelper.init(zkService);
		}
		JSONObject topologySummary = JSON.parseObject(StormToplogyOpHelper.topologySummary());
		JSONArray topologies = topologySummary.getJSONArray("topologies");
		for (int i = 0; i < topologies.size(); i++) {
			JSONObject topo = topologies.getJSONObject(i);
			String id = topo.getString("id");
			if (-1 != id.indexOf("global-splitter-puller")) {
				return id;
			}
		}
		return "";
	}

	public int killGlobalFullPullTopo() throws Exception {
		String topologyId = getGlobalFullPullTopo();
		String killResult = StormToplogyOpHelper.killTopology(topologyId, 10);
		if (org.apache.commons.lang.StringUtils.isNotBlank(killResult) && killResult.equals(StormToplogyOpHelper.OP_RESULT_SUCCESS)) {
			return ResultEntity.SUCCESS;
		} else {
			return MessageCode.DATASOURCE_KILL_TOPO_FAILED;
		}
	}

	public DataSource getDataSourceById(Integer id) {
		DataSource dataSource = null;
		dataSource = dataSourceMap.get(id);
		if (dataSource == null) {
			dataSource = sender.get(ServiceNames.KEEPER_SERVICE, "/datasource/{id}", id).getBody().getPayload(DataSource.class);
			dataSourceMap.put(id, dataSource);
		}
		return dataSource;
	}

	public List<DataSource> getDataSourceByDsTypes(List<String> dsTypes) {
		ResponseEntity<ResultEntity> result = sender.post(ServiceNames.KEEPER_SERVICE, "/datasource/getDataSourceByDsTypes", dsTypes);
		return result.getBody().getPayload(new TypeReference<List<DataSource>>() {
		});
	}

	public List<DataSource> getDataSourceByDsType(String dsType) {
		ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/datasource/getDataSourceByDsType?dsType=" + dsType);
		return result.getBody().getPayload(new TypeReference<List<DataSource>>() {
		});
	}

	/**
	 * 给所有数据线发送reloadMessage
	 *
	 * @param reloadType
	 * @return
	 */
	public Integer batchSendControMessage(String reloadType) {
		//oracle,mysql,mongo,db2,log_logstash,log_logstash_json,log_ums,log_flume,log_filebeat
		Integer result = 0;
		if (HEARTBEAT_RELOAD_CONFIG.equalsIgnoreCase(reloadType)) {
			reloadHeartBeat();
			logger.info("reload heartbeat success.");
			return 0;
		}
		ArrayList<String> dsTypeList = new ArrayList<>();
		List<DataSource> dataSourceList = null;
		if (FULL_DATA_PULL_RELOAD_CONF.equalsIgnoreCase(reloadType)
				|| DISPATCHER_RELOAD_CONFIG.equalsIgnoreCase(reloadType)
				|| APPENDER_RELOAD_CONFIG.equalsIgnoreCase(reloadType)) {
			dsTypeList.add("mysql");
			dsTypeList.add("oracle");
			dsTypeList.add("mongo");
			dataSourceList = getDataSourceByDsTypes(dsTypeList);
		} else if (EXTRACTOR_RELOAD_CONF.equalsIgnoreCase(reloadType)) {
			dsTypeList.add("mysql");
			dataSourceList = getDataSourceByDsTypes(dsTypeList);
		} else if (LOG_PROCESSOR_RELOAD_CONFIG.equalsIgnoreCase(reloadType)) {
			dataSourceList = getDataSourceByDsType("log");
		} else {
			return MessageCode.UNSUPPORTED_CTRL_MESSAGE_TYPES;
		}
		for (DataSource ds : dataSourceList) {
			result = sendCtrlMessage(reloadType, ds);
			if (result != 0) {
				return result;
			}
		}
		return result;
	}

	public int sendCtrlMessage(String reloadType, DataSource ds) {
		HashMap<String, String> map = makeCtrlMessageParam(ds.getCtrlTopic());
		JSONObject message = this.bulidMessage(ds, reloadType);
		map.put("message", message.toString());
		Integer result = this.sendCtrlMessage(map);
		if (result != 0) {
			return result;
		}
		logger.info("[{}] reload {} success.", ds.getDsName(), reloadType);
		return 0;
	}

}
