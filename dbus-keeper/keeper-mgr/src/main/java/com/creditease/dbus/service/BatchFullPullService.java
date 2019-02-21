package com.creditease.dbus.service;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.base.com.creditease.dbus.utils.RequestSender;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.ServiceNames;
import com.creditease.dbus.domain.model.DataTable;
import com.creditease.dbus.domain.model.FullPullHistory;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * This is Description
 *
 * @author xiancangao
 * @date 2019/02/19
 */
@Service
public class BatchFullPullService {
	protected Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	private RequestSender sender;
	@Autowired
	private IZkService zkService;
	@Autowired
	private FullPullService fullPullService;
	@Autowired
	private ProjectTableService projectTableService;

	@Async
	public void batchGlobalfullPull(Map<String, Object> map) throws Exception{
		logger.info("批量拉全量开始..........");
		logger.info("批量拉全量请求参数:{}", map);

		String outputTopic = (String)map.get("outputTopic");
		Boolean isProject = (Boolean) map.get("isProject");
		ArrayList<Integer> ids = (ArrayList<Integer>) map.get("ids");
		if(isProject){
			//处理多租户
			ResponseEntity<ResultEntity> result = sender.post(ServiceNames.KEEPER_SERVICE, "/projectTable/search-tables", ids);
			List<Map<String, Object>> topoTableList = result.getBody().getPayload(new TypeReference<List<Map<String, Object>>>() {
			});
			//根据schema分类
			Map<String, List<Map<String, Object>>> schemaTopoTables = new HashMap<>();
			for (Map<String, Object> topoTable : topoTableList) {
				String dsName = (String) topoTable.get("dsName");
				String schemaName = (String) topoTable.get("schemaName");
				List<Map<String, Object>> list = schemaTopoTables.get(dsName + "&" + schemaName);
				if (list == null) {
					list = new ArrayList<>();
				}
				list.add(topoTable);
				schemaTopoTables.put(dsName + "&" + schemaName, list);
			}
			for (Map.Entry<String, List<Map<String, Object>>> entry : schemaTopoTables.entrySet()) {
				List<Map<String, Object>> topoTables = entry.getValue();
				executeProjectfullPull(outputTopic, topoTables);
			}
		}else{
			//处理源端
			ResponseEntity<ResultEntity> result = sender.post(ServiceNames.KEEPER_SERVICE, "/tables/searchTableByIds", ids);
			List<DataTable> tableList = result.getBody().getPayload(new TypeReference<List<DataTable>>() {
			});
			Map<String, List<DataTable>> schemaTables = new HashMap<>();
			for (DataTable table : tableList) {
				String dsName = table.getDsName();
				String schemaName = table.getSchemaName();
				List<DataTable> list = schemaTables.get(dsName + "&" + schemaName);
				if (list == null) {
					list = new ArrayList<>();
				}
				list.add(table);
				schemaTables.put(dsName + "&" + schemaName, list);
			}
			for (Map.Entry<String, List<DataTable>> entry : schemaTables.entrySet()) {
				List<DataTable> tables = entry.getValue();
				executeSourcefullPull(outputTopic, tables);
			}
		}
		logger.info("批量拉全量结束..........");
	}

	/**
	 * 多租户批量拉全量
	 *
	 * @param topoTables
	 */
	public void executeProjectfullPull(String outputTopic, List<Map<String, Object>> topoTables) throws Exception {
		Map<String, Object> payloadMap = null;
		for (Map<String, Object> topoTable : topoTables) {
			Date date = new Date();
			JSONObject message = fullPullService.buildMessage(date);
			message = this.buildMessage(message, outputTopic, date.getTime(), topoTable);
			JSONObject payload = message.getJSONObject("payload");
			if (payloadMap == null) {
				String resultTopic = (String) topoTables.get(0).get("outputTopic");
				payloadMap = fullPullService.getOPTS(resultTopic, payload);
			}
			payload.put("POS", payloadMap.get("POS"));
			payload.put("OP_TS", payloadMap.get("OP_TS"));

			//生成fullPullHistory对象
			FullPullHistory fullPullHistory = new FullPullHistory();
			fullPullHistory.setId(date.getTime());
			fullPullHistory.setType("indepent");
			fullPullHistory.setDsName((String) topoTable.get("dsName"));
			fullPullHistory.setSchemaName((String) topoTable.get("schemaName"));
			fullPullHistory.setTableName((String) topoTable.get("tableName"));
			fullPullHistory.setState("init");
			fullPullHistory.setInitTime(date);
			fullPullHistory.setUpdateTime(fullPullHistory.getInitTime());
			fullPullHistory.setTargetSinkTopic(outputTopic);
			fullPullHistory.setProjectName((String) topoTable.get("projectName"));
			fullPullHistory.setTopologyTableId((Integer) topoTable.get("topoTableId"));
			fullPullHistory.setTargetSinkId((Integer) topoTable.get("sinkId"));
			//发送消息
			DataTable dataTable = new DataTable();
			dataTable.setSchemaName((String) topoTable.get("schemaName"));
			dataTable.setPhysicalTableRegex((String) topoTable.get("physicalTableRegex"));
			dataTable.setCtrlTopic((String) topoTable.get("ctrlTopic"));
			dataTable.setDsType((String) topoTable.get("dsType"));
			dataTable.setMasterUrl((String) topoTable.get("masterUrl"));
			dataTable.setDbusUser((String) topoTable.get("dbusUser"));
			dataTable.setDbusPassword((String) topoTable.get("dbusPwd"));
			dataTable.setOutputTopic((String) topoTable.get("outputTopic"));
			fullPullService.sendMessage(dataTable, message.toJSONString(), fullPullHistory);
		}
	}

	public JSONObject buildMessage(JSONObject message, String outputTopic, long time, Map<String, Object> topoTable) {
		JSONObject payload = new JSONObject();
		payload.put("DBUS_DATASOURCE_ID", topoTable.get("dsId"));
		payload.put("SCHEMA_NAME", topoTable.get("schemaName"));
		payload.put("TABLE_NAME", topoTable.get("tableName"));
		payload.put("INCREASE_VERSION", "false");
		payload.put("INCREASE_BATCH_NO", "false");
		payload.put("resultTopic", outputTopic);
		payload.put("SEQNO", String.valueOf(time));
		payload.put("PHYSICAL_TABLES", topoTable.get("physicalTableRegex"));
		payload.put("PULL_REMARK", "");
		payload.put("SPLIT_BOUNDING_QUERY", "");
		payload.put("PULL_TARGET_COLS", "");
		payload.put("SCN_NO", "");
		payload.put("SPLIT_COL", topoTable.get("fullpullCol"));
		payload.put("SPLIT_SHARD_SIZE", topoTable.get("fullpullSplitShardSize"));
		payload.put("SPLIT_SHARD_STYLE", topoTable.get("fullpullSplitStyle"));
		payload.put("INPUT_CONDITIONS", topoTable.get("fullpullCondition"));

		JSONObject project = new JSONObject();
		project.put("id", topoTable.get("projectId"));
		project.put("name", topoTable.get("projectName"));
		project.put("sink_id", topoTable.get("sinkId"));
		project.put("topo_table_id", topoTable.get("topoTableId"));

		message.put("payload", payload);
		message.put("project", project);
		return message;
	}

	/**
	 * 源端批量拉全量
	 *
	 * @param tables
	 */
	public void executeSourcefullPull(String outputTopic, List<DataTable> tables) throws Exception {
		Map<String, Object> payloadMap = null;
		for (DataTable table : tables) {
			JSONObject message = fullPullService.buildSourceFullPullMessage(table, outputTopic);
			JSONObject payload = message.getJSONObject("payload");
			if (payloadMap == null) {
				String resultTopic = tables.get(0).getOutputTopic();
				payloadMap = fullPullService.getOPTS(resultTopic, payload);
			}
			payload.put("POS", payloadMap.get("POS"));
			payload.put("OP_TS", payloadMap.get("OP_TS"));

			//生成fullPullHistory对象
			FullPullHistory fullPullHistory = new FullPullHistory();
			fullPullHistory.setId(message.getLong("id"));
			fullPullHistory.setType("indepent");
			fullPullHistory.setDsName(table.getDsName());
			fullPullHistory.setSchemaName(table.getSchemaName());
			fullPullHistory.setTableName(table.getTableName());
			fullPullHistory.setState("init");
			fullPullHistory.setInitTime(new Date(fullPullHistory.getId()));
			fullPullHistory.setUpdateTime(fullPullHistory.getInitTime());
			fullPullHistory.setTargetSinkTopic(outputTopic);
			//发送消息
			fullPullService.sendMessage(table, message.toJSONString(), fullPullHistory);
		}
	}
}
