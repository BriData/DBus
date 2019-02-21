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
import com.creditease.dbus.bean.SourceTablesBean;
import com.creditease.dbus.commons.log.processor.parse.RuleGrammar;
import com.creditease.dbus.commons.log.processor.rule.impl.Rules;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.constant.ServiceNames;
import com.creditease.dbus.domain.mapper.*;
import com.creditease.dbus.domain.model.*;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.service.meta.MetaFetcher;
import com.creditease.dbus.service.source.SourceFetcher;
import com.creditease.dbus.service.table.MongoTableFetcher;
import com.creditease.dbus.service.table.TableFetcher;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.SystemUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import scala.Int;

import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

/**
 * Created by xiancangao on 2018/05/16.
 */
@Service
public class TableService {
	@Autowired
	private DataTableMapper tableMapper;

	@Autowired
	private TableMetaMapper tableMetaMapper;

	@Autowired
	private EncodePluginsService encodePluginsService;

	@Autowired
	private DesensitizationInformationMapper desMapper;

	@Autowired
	private DataTableRuleMapper dataTableRuleMapper;

	@Autowired
	private TableVersionMapper tableVersionMapper;

	@Autowired
	private DataSourceService dataSourceService;

	private static Logger logger = LoggerFactory.getLogger(TableService.class);

	private static final String NONE = "无";

	//默认的table 名称
	public static final String TABLE_FULL_PULL_LOWER = "db_full_pull_requests";
	public static final String TABLE_HB_MONITOR_LOWER = "db_heartbeat_monitor";
	public static final String TABLE_HB_MONITOR_UPPER = "DB_HEARTBEAT_MONITOR";
	public static final String TABLE_META_SYNC_EVENT = "META_SYNC_EVENT";
	public static final String TABLE_FULL_PULL_UPPER = "DB_FULL_PULL_REQUESTS";


	private static final String OUTPUT_TOPIC_SUFFIX_LOWER = ".dbus.result";//构造output的后缀
	private static final String OUTPUT_TOPIC_SUFFIX_UPPER = ".DBUS.result";

	private static final int INIT_VERSION = 0;//一些版本的初始值

	public List<DataTable> getTables(Integer dsId, String schemaName, String tableName) {
		return tableMapper.findTables(dsId, StringUtils.trim(schemaName), StringUtils.trim(tableName));
	}

	public PageInfo<DataTable> getTablesInPages(Integer dsId, String schemaName, String tableName, int pageNum, int pageSize) {
		PageHelper.startPage(pageNum, pageSize);
		List<DataTable> tables = tableMapper.findTables(dsId, StringUtils.trim(schemaName), StringUtils.trim(tableName));
		for (DataTable dataTable : tables) {
			String verChangeHistory = dataTable.getVerChangeHistory();
			if (StringUtils.isEmpty(verChangeHistory)) continue;
			StringBuilder versionsChangeHistory = new StringBuilder();
			for (String verId : verChangeHistory.split(",")) {
				try {
					TableVersion tableVersion = tableVersionMapper.selectByPrimaryKey(Integer.parseInt(verId));
					if (tableVersion != null && tableVersion.getVersion() != null) {
						versionsChangeHistory.append(",").append(tableVersion.getVersion());
					}
				} catch (NumberFormatException e) {
					logger.warn("transform verId to version failed for verid: {}, error message: {}", verId, e);
					versionsChangeHistory.append(",").append(verId);
				}
			}
			if (versionsChangeHistory.length() > 0) {
				versionsChangeHistory.delete(0, 1);
			}
			dataTable.setVerChangeHistory(versionsChangeHistory.toString());
		}
		return new PageInfo<>(tables);
	}

	public ResultEntity fetchTableColumns(Integer tableId) throws Exception {
		ResultEntity re = new ResultEntity();
		DataTable table = tableMapper.findById(tableId);
		if (table == null) {
			logger.info("tables : can not found table by id.");
			re.setStatus(MessageCode.TABLE_NOT_FOUND_BY_ID);
			re.setMessage("传入ID不正确,找不到相关表");
			return re;
		}

		DataSource dataSource = new DataSource();
		dataSource.setDsType(table.getDsType());
		dataSource.setMasterUrl(table.getMasterUrl());
		dataSource.setDbusUser(table.getDbusUser());
		dataSource.setDbusPwd(table.getDbusPassword());

		TableFetcher fetcher = null;
		try {
			fetcher = TableFetcher.getFetcher(dataSource);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			re.setStatus(MessageCode.CAN_NOT_FETCH_TABLE_COLUMNS_INFORMATION);
			re.setMessage("该表数据类型暂时不支持查询操作");
			return re;
		}
		List<Map<String, Object>> columnsInfomation = fetcher.fetchTableColumnsInformation(table);
		if (columnsInfomation.isEmpty()) {
			logger.error("无法获取表结构信息");
			re.setStatus(MessageCode.CAN_NOT_FETCH_TABLE_COLUMNS_INFORMATION);
			re.setMessage("传入ID不正确,找不到表字段相关信息");
			return re;
		}
		re.setPayload(columnsInfomation);
		return re;
	}

	public void update(DataTable dataTable) {
		tableMapper.updateByPrimaryKey(dataTable);
	}

	public int deleteTable(int tableId) throws Exception{
		DataTable table = getTableById(tableId);
		//删除表数据
		tableMapper.deleteByTableId(tableId);
		//oracle类型需要删除源端DBUS_TABLES对应的表
		if(table.getDsType().equalsIgnoreCase("oracle")){
			HashMap<String, Object> params = new HashMap<>();
			params.put("dsType", table.getDsType());
			params.put("URL", table.getMasterUrl());
			params.put("user", table.getDbusUser());
			params.put("password", table.getDbusPassword());
			SourceFetcher sourceFetcher = SourceFetcher.getFetcher(params);
			this.deleteDbusTables(sourceFetcher,table);
		}
		return 0;
	}

	public int confirmStatusChange(int tableId) {
		return tableMapper.confirmVerChange(tableId);
	}

	public Map<String, List<Map<String, Object>>> getVersionDetail(Integer versionId1, Integer versionId2) {
		List<Map<String, Object>> result1 = tableMetaMapper.getVersionColumnDetail(versionId1);
		List<Map<String, Object>> result2 = tableMetaMapper.getVersionColumnDetail(versionId2);

		Map<String, List<Map<String, Object>>> result = new HashMap<>();
		result.put("v1data", result1);
		result.put("v2data", result2);
		return result;
	}

	public List<DesensitizationInformation> getDesensitizationInfo(Integer tableId) {
		return desMapper.getDesensitizationInfoByTableId(tableId);
	}

	public DataTable getTableById(int id) {
		return tableMapper.findById(id);
	}

	public List<DataTable> findByStatus(String status) {
		return tableMapper.findByStatus(status);
	}

	public void changeDesensitization(Map<String, Map<String, Object>> param) {
		for (String key : param.keySet()) {
			Map<String, Object> map = param.get(key);
			DesensitizationInformation di = new DesensitizationInformation();
			if (map.get("sql_type").equals("update")) {
				di.setId(Integer.valueOf(map.get("id").toString()));
				di.setTableId(Integer.valueOf(map.get("table_id").toString()));
				di.setFieldName(map.get("field_name").toString());
				di.setEncodeType(map.get("encode_type").toString());
				di.setEncodeParam(map.get("encode_param").toString());
				di.setTruncate(Integer.valueOf(map.get("truncate").toString()));
				//添加pluginId
				di.setPluginId(Integer.valueOf(map.get("plugin_id").toString()));
				di.setUpdateTime(new Date(Long.valueOf(map.get("update_time").toString())));
				desMapper.updateByPrimaryKey(di);
			} else if (map.get("sql_type").equals("delete")) {
				desMapper.deleteByPrimaryKey(Integer.valueOf(map.get("id").toString()));
			} else if (map.get("sql_type").equals("insert")) {
				di.setTableId(Integer.valueOf(map.get("table_id").toString()));
				di.setFieldName(map.get("field_name").toString());
				di.setEncodeType(map.get("encode_type").toString());
				di.setEncodeParam(map.get("encode_param").toString());
				di.setTruncate(Integer.valueOf(map.get("truncate").toString()));
				//添加pluginId
				di.setPluginId(Integer.valueOf(map.get("plugin_id").toString()));
				di.setUpdateTime(new Date(Long.valueOf(map.get("update_time").toString())));
				desMapper.insert(di);
			}
		}
	}

	public Map<String, Object> getAllRuleGroup(Integer tableId) {
		List<DataTableRuleGroup> allRuleGroup = dataTableRuleMapper.getAllRuleGroup(tableId);
		List<RuleInfo> ruleInfos = dataTableRuleMapper.getAsRuleInfo(tableId, Rules.SAVEAS.name);

		Map<String, Object> map = new HashMap<>();
		map.put("group", allRuleGroup);
		map.put("saveAs", ruleInfos);
		return map;
	}

	public int updateRuleGroup(Map<String, Object> map) {
		DataTableRuleGroup group = new DataTableRuleGroup();
		group.setId(Integer.parseInt(map.get("groupId").toString()));
		group.setStatus(map.get("newStatus").toString());
		group.setGroupName(map.get("newName").toString());

		int id = dataTableRuleMapper.updateRuleGroup(group);
		return id;
	}

	public void deleteRuleGroup(Integer groupId) {
		dataTableRuleMapper.deleteRuleGroup(groupId);
		dataTableRuleMapper.deleteRules(groupId);
	}

	public int addGroup(Map<String, Object> map) {
		DataTableRuleGroup group = new DataTableRuleGroup();
		group.setTableId(Integer.parseInt(map.get("tableId").toString()));
		group.setStatus(map.get("newStatus").toString());
		group.setGroupName(map.get("newName").toString());
		return dataTableRuleMapper.addGroup(group);
	}

	public Map<String, Object> cloneRuleGroup(Map<String, Object> map) throws Exception {
		int tableId = Integer.parseInt(map.get("tableId").toString());
		final int oldGroupId = Integer.parseInt(map.get("groupId").toString());
		// 生成新的规则组信息
		DataTableRuleGroup group = new DataTableRuleGroup();
		group.setTableId(tableId);
		group.setGroupName(map.get("newName").toString());
		group.setStatus(map.get("newStatus").toString());

		int insertResult = dataTableRuleMapper.addGroup(group);
		if (insertResult == 0) {
			throw new SQLException();
		}
		List<DataTableRule> rules = dataTableRuleMapper.getAllRules(oldGroupId);
		if (rules != null && rules.size() > 0) {
			//修改规则的groupId
			for (DataTableRule rule : rules) {
				rule.setGroupId(group.getId());
			}
			insertResult = dataTableRuleMapper.saveAllRules(rules);
			if (insertResult != rules.size()) {
				throw new SQLException();
			}
		}

		List<DataTableRuleGroup> allRuleGroup = dataTableRuleMapper.getAllRuleGroup(tableId);
		List<RuleInfo> ruleInfos = dataTableRuleMapper.getAsRuleInfo(tableId, Rules.SAVEAS.name);

		Map<String, Object> result = new HashMap<>();
		result.put("group", allRuleGroup);
		result.put("saveAs", ruleInfos);
		return result;
	}

	public List<RuleInfo> diffGroupRule(Integer tableId) {
		return dataTableRuleMapper.getAsRuleInfo(tableId, Rules.SAVEAS.name);
	}

	public String upgradeVersion(Map<String, Object> map) {
		String validateResult = validateAs(map);
		if (StringUtils.isNotEmpty(validateResult)) {
			return validateResult;
		}
		/**
		 * 在t_meta_version中生成新版本号，同时更新t_data_table中的版本
		 */
		int verId = generateNewVerId(map);
		/**
		 * 从t_plain_log_rule_group和t_plain_log_rules中复制规则到
		 * t_plain_log_rule_group_version和t_plain_log_rules_version中
		 */
		confirmUpgradeVersion(map, verId);
		/**
		 * 向t_table_meta中插入 ruleScope,name,type
		 */
		upgradeRuleTableMeta(map, verId);
		return validateResult;
	}

	private String validateAs(Map<String, Object> map) {
		final String INACTIVE = "inactive";
		List<RuleInfo> ruleInfos = dataTableRuleMapper.getAsRuleInfo(Integer.parseInt(map.get("tableId").toString()), Rules.SAVEAS.name);
		for (int i = 0; i < ruleInfos.size(); i++) {
			if (ruleInfos.get(i).getStatus().equals(INACTIVE)) continue;
			if (StringUtils.isEmpty(ruleInfos.get(i).getRuleGrammar()))
				return ruleInfos.get(i).getGroupName();
			for (int j = i + 1; j < ruleInfos.size(); j++) {
				if (ruleInfos.get(j).getStatus().equals(INACTIVE)) continue;
				if (StringUtils.isEmpty(ruleInfos.get(j).getRuleGrammar()))
					return ruleInfos.get(j).getGroupName();
				List<RuleGrammar> ruleGrammar1 = JSON.parseArray(ruleInfos.get(i).getRuleGrammar(), RuleGrammar.class);
				List<RuleGrammar> ruleGrammar2 = JSON.parseArray(ruleInfos.get(j).getRuleGrammar(), RuleGrammar.class);
				if (!compareRuleGrammarAsType(ruleGrammar1, ruleGrammar2)) {
					return ruleInfos.get(i).getGroupName() + " 和 " + ruleInfos.get(j).getGroupName();
				}
			}
		}
		return null;
	}

	private boolean compareRuleGrammarAsType(List<RuleGrammar> ruleGrammar1, List<RuleGrammar> ruleGrammar2) {
		if (ruleGrammar1 == null || ruleGrammar2 == null) return false;
		if (ruleGrammar1.size() != ruleGrammar2.size()) return false;
		Collections.sort(ruleGrammar1, (o1, o2) -> StringUtils.compare(o1.getName(), o2.getName()));
		Collections.sort(ruleGrammar2, (o1, o2) -> StringUtils.compare(o1.getName(), o2.getName()));
		for (int i = 0; i < ruleGrammar1.size(); i++) {
			if (!StringUtils.equals(ruleGrammar1.get(i).getName(), ruleGrammar2.get(i).getName())) return false;
			if (!StringUtils.equals(ruleGrammar1.get(i).getType(), ruleGrammar2.get(i).getType())) return false;
			if (!StringUtils.equals(ruleGrammar1.get(i).getRuleScope(), ruleGrammar2.get(i).getRuleScope()))
				return false;
		}
		return true;
	}

	private int generateNewVerId(Map<String, Object> map) {
		int tableId = Integer.parseInt(map.get("tableId").toString());
		int verId;
		TableVersion tableVersion = tableVersionMapper.getVersionByTableId(tableId);
		if (tableVersion != null) {
			tableVersion.setId(null);
			tableVersion.setUpdateTime(new Timestamp(System.currentTimeMillis()));
			tableVersion.setVersion(tableVersion.getVersion() + 1);
			tableVersion.setInnerVersion(tableVersion.getInnerVersion() + 1);
			tableVersionMapper.insert(tableVersion);
			verId = tableVersion.getId();
		} else {
			TableVersion tv = new TableVersion();
			tv.setTableId(tableId);
			tv.setDsId(Integer.parseInt(map.get("dsId").toString()));
			tv.setDbName(map.get("dsName").toString());
			tv.setSchemaName(map.get("schemaName").toString());
			tv.setTableName(map.get("tableName").toString());
			tv.setVersion(0);
			tv.setInnerVersion(0);
			tv.setEventOffset(0L);
			tv.setEventPos(0L);
			tv.setUpdateTime(new Timestamp(System.currentTimeMillis()));

			tableVersionMapper.insert(tv);
			verId = tableVersion.getId();
		}
		tableMapper.updateVerId(verId, tableId);
		return verId;
	}

	private void confirmUpgradeVersion(Map<String, Object> map, int verId) {
		int tableId = Integer.parseInt(map.get("tableId").toString());
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		List<DataTableRuleGroup> dataTableRuleGroupList = dataTableRuleMapper.getActiveRuleGroupByTableId(tableId);

		for (DataTableRuleGroup dataTableRuleGroup : dataTableRuleGroupList) {
			List<DataTableRule> allRules = dataTableRuleMapper.getAllRules(dataTableRuleGroup.getId());

			HashMap<String, Object> ruleGroupVersion = new HashMap<>();
			ruleGroupVersion.put("table_id", tableId);
			ruleGroupVersion.put("group_name", dataTableRuleGroup.getGroupName());
			ruleGroupVersion.put("status", dataTableRuleGroup.getStatus());
			ruleGroupVersion.put("ver_id", verId);
			ruleGroupVersion.put("update_time", timestamp);
			dataTableRuleMapper.insertRuleGroupVersion(ruleGroupVersion);
			int groupId = Integer.parseInt(ruleGroupVersion.get("id").toString());

			for (DataTableRule dataTableRule : allRules) {
				HashMap<String, Object> rulesVersion = new HashMap<>();
				rulesVersion.put("group_id", groupId);
				rulesVersion.put("order_id", dataTableRule.getOrderId());
				rulesVersion.put("rule_type_name", dataTableRule.getRuleTypeName());
				rulesVersion.put("rule_grammar", dataTableRule.getRuleGrammar());
				rulesVersion.put("update_time", timestamp);
				dataTableRuleMapper.insertRulesVersion(rulesVersion);
			}
		}
	}

	private void upgradeRuleTableMeta(Map<String, Object> map, int verId) {
		List<RuleInfo> ruleInfos = dataTableRuleMapper.getAsRuleInfo(Integer.parseInt(map.get("tableId").toString()), Rules.SAVEAS.name);
		RuleInfo asRuleInfo = null;
		for (RuleInfo ruleInfo : ruleInfos) {
			if ("active".equals(ruleInfo.getStatus())) asRuleInfo = ruleInfo;
		}
		if (asRuleInfo == null) return;
		List<RuleGrammar> ruleGrammars = JSON.parseArray(asRuleInfo.getRuleGrammar(), RuleGrammar.class);
		for (RuleGrammar ruleGrammar : ruleGrammars) {
			TableMeta tableMeta = new TableMeta();
			tableMeta.setVerId(verId);
			tableMeta.setOriginalColumnName(ruleGrammar.getName());
			tableMeta.setColumnName(ruleGrammar.getName());
			tableMeta.setColumnId(Integer.parseInt(ruleGrammar.getRuleScope()));
			tableMeta.setInternalColumnId(Integer.parseInt(ruleGrammar.getRuleScope()));
			tableMeta.setOriginalSer(0);
			tableMeta.setDataType(ruleGrammar.getType());
			tableMeta.setDataLength(0L);
			tableMeta.setDataPrecision(0);
			tableMeta.setDataScale(0);
			tableMeta.setNullable("Y");
			tableMeta.setIsPk("N");
			tableMeta.setPkPosition(-1);
			tableMetaMapper.insert(tableMeta);
		}
	}

	public List<DataTableRule> getAllRules(Integer groupId) {
		return dataTableRuleMapper.getAllRules(groupId);
	}

	public void saveAllRules(Map<String, Object> map) {
		DataTableRuleGroup group = new DataTableRuleGroup();
		group.setId(Integer.parseInt(map.get("groupId").toString()));
		dataTableRuleMapper.updateRuleGroup(group);
		dataTableRuleMapper.deleteRules(Integer.parseInt(map.get("groupId").toString()));

		String listTxt = map.get("rules").toString();
		List<DataTableRule> list = JSONArray.parseArray(listTxt, DataTableRule.class);
		dataTableRuleMapper.saveAllRules(list);
	}

	public List<DataTable> getTablesBySchemaID(Integer id) {
		return tableMapper.findBySchemaID(id);
	}

	public List<Map<String, Object>> getDSList() {
		return tableMapper.getDSList();
	}

	public DataTable getById(Integer tableId) {
		return tableMapper.findById(tableId);
	}

	public List<Map<String, Object>> executeSql(Map<String, Object> map) throws Exception {
		DataSource ds = new DataSource();
		ds.setDsType(map.get("dsType").toString());
		ds.setMasterUrl(map.get("URL").toString());
		ds.setDbusUser(map.get("user").toString());
		ds.setDbusPwd(map.get("password").toString());
		TableFetcher fetcher = TableFetcher.getFetcher(ds);
		return fetcher.fetchTableColumn(map);
	}

	public List<DataTable> getSchemaTables(int dsId, String schemaName) {
		return tableMapper.findByDsIdAndSchemaName(dsId, StringUtils.trim(schemaName));
	}

	/**
	 * 获取源端表，主要包含tablenames信息
	 *
	 * @return list: 正确结果 | null: 异常 | Exception：参数类型不对
	 */
	private List<DataTable> fetchTables(int dsId, String schemaName) throws Exception {
		try {
			DataSource dataSource = dataSourceService.getById(dsId);
			String dsName = dataSource.getDsName();
			List<DataTable> list;

			if (DbusDatasourceType.stringEqual(dataSource.getDsType(), DbusDatasourceType.MYSQL)
					|| DbusDatasourceType.stringEqual(dataSource.getDsType(), DbusDatasourceType.ORACLE)
					) {
				TableFetcher fetcher = TableFetcher.getFetcher(dataSource);
				Map<String, Object> map = new HashMap<>();
				map.put("dsName", dsName);
				map.put("schemaName", schemaName);
				list = fetcher.fetchTable(map);
			} else if (DbusDatasourceType.stringEqual(dataSource.getDsType(), DbusDatasourceType.MONGO)) {
				MongoTableFetcher fetcher = new MongoTableFetcher(dataSource);
				list = fetcher.fetchTable(schemaName);
			} else {
				throw new IllegalArgumentException("Unsupported datasource type");
			}

			return list;
		} catch (Exception e) {
			logger.error("[fetch tables] dsId:{}, schemaName: {}, Catch exception: {}", dsId, schemaName, e);
			throw e;
		}
	}

	/**
	 * 获取源端的table信息，包括Incompatible Column等信息
	 *
	 * @param paramsList
	 * @return
	 */
	private List<List<TableMeta>> fetchTableField(List<Map<String, Object>> paramsList) throws Exception {
		List<List<TableMeta>> ret = new ArrayList<>();

		for (Map<String, Object> params : paramsList) {
			try {
				String dsName = String.valueOf(params.get("dsName"));

				List<DataSource> dataSourceList = dataSourceService.getDataSourceByName(dsName);
				if (dataSourceList.size() == 0) {
					// return
					logger.error("[Fetch table field] table not found. dsName:{}", dsName);
				}
				DataSource dataSource = dataSourceList.get(0);
				//根据不同类型的dataSource，来选择不同的TableFetcher
				if (DbusDatasourceType.stringEqual(dataSource.getDsType(), DbusDatasourceType.MYSQL)
						|| DbusDatasourceType.stringEqual(dataSource.getDsType(), DbusDatasourceType.ORACLE)
						) {
					TableFetcher fetcher = TableFetcher.getFetcher(dataSource);
					ret = fetcher.fetchTableField(params, paramsList);

				} else if (DbusDatasourceType.stringEqual(dataSource.getDsType(), DbusDatasourceType.MONGO)) {
					MongoTableFetcher fetcher = new MongoTableFetcher(dataSource);
					ret = fetcher.fetchTableField(params, paramsList);
				} else {
					throw new IllegalArgumentException("Unsupported datasource type");
				}
				break;

			} catch (Exception e) {
				logger.error("[Fetch table field] Fetch failed, error message: {}", e);
				throw e;
			}
		}
		return ret;
	}

	public List<Map<String, Object>> tableList(int dsId, String dsName, String schemaName) throws Exception {
		try {
			/* 查询源库中所有table信息 */
			List<DataTable> sourceTables = fetchTables(dsId, schemaName);


            /*构造[ {}，{"dsName","schemaName","tableNames"}，{} ] 的参数集合
              由于fetchTables只能获取tableName信息，没有dsName和schemaName，所以这两个需要传入
             */
			List<Map<String, Object>> nameParmas = new ArrayList<>();
			for (DataTable dataTable : sourceTables) {
				Map<String, Object> nameMap = new HashedMap();
				nameMap.put("dsName", dsName);
				nameMap.put("schemaName", schemaName);
				nameMap.put("tableName", dataTable.getTableName());

				nameParmas.add(nameMap);
			}
			// 查询源库的table meta信息
			List<List<TableMeta>> sourceTableMetas = fetchTableField(nameParmas);


			// 查询管理库中table信息（已添加的table）
			List<DataTable> managerTables = getSchemaTables(dsId, schemaName);

			// 定义最后返回的结果集
			List<Map<String, Object>> resultTableInfo = new ArrayList<>();

			for (int i = 0; i < sourceTables.size(); i++) {
				List<TableMeta> meta = sourceTableMetas.get(i);
				int metaLength = meta.size();
				String sourceTableName = sourceTables.get(i).getTableName();
				// boolean __ckbox_checked__ = false;
				boolean disable = false;
				String ignoreColumnName = "";
				String incompatibleColumn = "";
				String physicalTableRegex = "";
				String outputTopic = "";

				//对比两个结果，已添加的表需要赋值和 disable = true;
				for (int j = 0; j < managerTables.size(); j++) {
					if (StringUtils.equals(sourceTableName, managerTables.get(j).getTableName())) {
						//__ckbox_checked__ = true;
						disable = true;
						physicalTableRegex = managerTables.get(j).getPhysicalTableRegex();
						outputTopic = managerTables.get(j).getOutputTopic();
						break;
					}
				}

				if (metaLength > 0) {
					for (TableMeta index : meta) {
						ignoreColumnName += index.getColumnName() + '/' + index.getDataType() + ' ';
						if (index.getIncompatibleColumn() != null) {
							incompatibleColumn += index.getIncompatibleColumn() + ' ';
						}
					}
					if (StringUtils.isEmpty(incompatibleColumn))
						incompatibleColumn = NONE;

					Map<String, Object> resultTableMap = new HashedMap();
					resultTableMap.put("tableName", sourceTables.get(i).getTableName());
					resultTableMap.put("physicalTableRegex", physicalTableRegex);
					resultTableMap.put("outputTopic", outputTopic);
					resultTableMap.put("columnName", ignoreColumnName);
					resultTableMap.put("incompatibleColumn", incompatibleColumn);
					resultTableMap.put("disable", disable);

					resultTableInfo.add(resultTableMap);
				} else {
					Map<String, Object> resultTableMap = new HashedMap();
					resultTableMap.put("tableName", sourceTables.get(i).getTableName());
					resultTableMap.put("physicalTableRegex", physicalTableRegex);
					resultTableMap.put("outputTopic", outputTopic);
					resultTableMap.put("columnName", NONE);
					resultTableMap.put("incompatibleColumn", NONE);
					resultTableMap.put("disable", disable);

					resultTableInfo.add(resultTableMap);

				}
			}
			return resultTableInfo;
		} catch (Exception e) {
			logger.error("[tables to add] Exception: ", e);
			throw e;
		}
	}

	public DataTable getByDsSchemaTableName(String dsName, String schemaName, String tableName) {
		return tableMapper.getByDsSchemaTableName(dsName, schemaName, tableName);
	}

	/**
	 * 加表时，将表的相关信息插入源端的库
	 */
	public ResultEntity sourceTablesHandler(SourceTablesBean sourceTablesInfo) throws Exception {
		int dsId = sourceTablesInfo.getDsId();
		List<DataTable> sourceTables = sourceTablesInfo.getSourceTables();
		return sourceTablesHandler(dsId, sourceTables);
	}

	private ResultEntity sourceTablesHandler(int dsId, List<DataTable> tableInfos) throws Exception {
		ResultEntity errorResult = new ResultEntity(0, null);
		try {
			DataSource dataSource = dataSourceService.getById(dsId);
			String dsType = dataSource.getDsType();
			if (tableInfos.size() > 0 && StringUtils.equals(dsType, "oracle")) {

				HashMap<String, Object> params = new HashMap<>();
				params.put("dsType", dataSource.getDsType());
				params.put("URL", dataSource.getMasterUrl());
				params.put("user", dataSource.getDbusUser());
				params.put("password", dataSource.getDbusPwd());
				SourceFetcher sourceFetcher = SourceFetcher.getFetcher(params);

				//1.校验是否打开全量补充日志
				int result = isOpenSupplementalLog(sourceFetcher, tableInfos);
				if (result != 0) {
					errorResult.setStatus(result);
					return errorResult;
				}

				//2.获取源端已添加表的列表
				List<String> sourceTableList = sourceFetcher.listTable();

				//3.去除源端已添加的表
				Iterator<DataTable> iterator = tableInfos.iterator();
				while (iterator.hasNext()) {
					DataTable table = iterator.next();
					String schemaName = table.getSchemaName();
					String tableName = table.getTableName();
					if (sourceTableList.contains(schemaName + "." + tableName)) {
						iterator.remove();
					}
				}
				//4.把要添加的表插入源端库
				if (0 != insertSourceTable(sourceFetcher, tableInfos)) {
					errorResult.setStatus(MessageCode.SOURCE_TABLE_INSERT_ERROR);
				}
				sourceFetcher.close();
			}
			return errorResult;
		} catch (Exception e) {
			logger.error("[insert table in resource]Exception: {}", e);
			errorResult.setStatus(MessageCode.SOURCE_TABLE_INSERT_ERROR);
			return errorResult;
		}

	}

	/**
	 * 查询是否打开全量补充日志
	 * @param fetcher
	 * @param tableInfos
	 * @return
	 * @throws Exception
	 */
	private int isOpenSupplementalLog(SourceFetcher fetcher, List<DataTable> tableInfos) throws Exception {
		int result = 0;
		ArrayList<String> list = fetcher.getSupplementalLog();
		String schemaAndTableName = null;
		for (DataTable table : tableInfos) {
			schemaAndTableName = table.getSchemaName() + "." + table.getTableName();
			if (!list.contains(schemaAndTableName)) {
				logger.error("SupplementalLog for table [{}] is not open", schemaAndTableName);
				result = MessageCode.TABLE_SUPPLEMENTAL_LOG_NOT_OPEN;
			}
		}
		return result;
	}

	/**
	 * 往源端表中插入新的table信息：最后插入的步骤
	 *
	 * @throws Exception
	 */
	private int insertSourceTable(SourceFetcher sourceFetcher, List<DataTable> tableInfos) {
		try {
			if (tableInfos == null || tableInfos.size() == 0) {
				return 0;
			}
			StringBuilder sb = new StringBuilder("INSERT ALL");
			for (DataTable table : tableInfos) {
				sb.append(" INTO DBUS.DBUS_TABLES VALUES( '").append(table.getSchemaName()).append("','").append(table.getTableName()).append("',sysdate) ");
			}
			sb.append("SELECT 1 FROM DUAL");
			logger.info("insert dbus_tables sql: {}", sb.toString());
			sourceFetcher.executeSql(sb.toString());
			return 0;
		} catch (Exception e) {
			logger.error("Exception when insert oracle tables to source dbus_tables.", e);
			return -1;
		}
	}

	/**
	 * 删除源端表中插入的table信息
	 *
	 * @throws Exception
	 */
	private int deleteDbusTables(SourceFetcher sourceFetcher, DataTable table) {
		try {
			String sql = "DELETE FROM DBUS_TABLES WHERE OWNER='" + table.getSchemaName() + "' AND  TABLE_NAME='" + table.getTableName() + "'";
			logger.info("delete dbus_tables sql: {}", sql);
			sourceFetcher.executeSql(sql);
			return 0;
		} catch (Exception e) {
			logger.error("Exception when delete oracle tables to source dbus_tables.", e);
			return -1;
		}
	}

	private DataTable findBySchemaIdAndTableName(int schemaId, String tableName) {
		return tableMapper.findBySchemaIdTableName(schemaId, tableName);
	}


	/**
	 * 管理库中插入表
	 *
	 * @param newTable
	 * @return 根据shcemaId和tableName校验：
	 * 如果存在，直接返回 0；
	 * 否则，添加后返回tableId；
	 * 异常 返回-1
	 */
	public int insertManageTable(DataTable newTable) {
		Integer schemaId = newTable.getSchemaId();
		if (schemaId == null) {
			logger.error("[insert manage table] SchemaId is null! Table:{}", newTable);
			return -1;
		}
		String tableName = newTable.getTableName();
		//table 已存在，不添加
		DataTable table = findBySchemaIdAndTableName(schemaId, tableName);
		if (findBySchemaIdAndTableName(schemaId, tableName) != null) {
			newTable.setId(table.getId());
			logger.info("[insert manage table] Table already exists. schemaId:{},  tableName:{}", schemaId, tableName);
			return table.getId();
		}
		//插入table
		newTable.setCreateTime(new Timestamp(System.currentTimeMillis()));
		tableMapper.insert(newTable);

		//t_data_tables中没有dsName等信息，查询后可获得 不插入。
		newTable = getById(newTable.getId());
		DbusDatasourceType dsType = DbusDatasourceType.parse(newTable.getDsType());
		//此处默认，非MySQL和Oracle的则为log类型，需要处理version信息;MySQL和Oracle的meta和version信息由增量处理
		if (DbusDatasourceType.MYSQL != dsType && DbusDatasourceType.ORACLE != dsType
				) {
			//插入table后，将table的version放入t_meta_verion中
			TableVersion tableVersion = new TableVersion();
			tableVersion.setTableId(newTable.getId());
			tableVersion.setDsId(newTable.getDsId());
			tableVersion.setDbName(newTable.getDsName());
			tableVersion.setSchemaName(newTable.getSchemaName());
			tableVersion.setTableName(newTable.getTableName());
			tableVersion.setVersion(INIT_VERSION);
			tableVersion.setInnerVersion(INIT_VERSION);
			tableVersion.setEventOffset(0L);
			tableVersion.setEventPos(0L);
			tableVersionMapper.insert(tableVersion);

			//meta插入完毕后，更新table的verId
			newTable.setVerId(tableVersion.getId());
			tableMapper.updateByPrimaryKey(newTable);
		}

		return newTable.getId();
	}

	/**
	 * 批量添加table
	 *
	 * @return 如果某个表添加出错，直接return -1;
	 */
	public int insertManageTable(List<DataTable> newTables) throws Exception {
		for (DataTable table : newTables) {
			int id = insertManageTable(table);
			if (id == -1) {
				return -1;
			}
			logger.info("insert newTable [{}] success.", table.getTableName());
		}
		initTableMeta(newTables);
		return 0;
	}

	private void initTableMeta(List<DataTable> newTables) throws Exception {
		DataSource dataSource = dataSourceService.getById(newTables.get(0).getDsId());
		String dsType = dataSource.getDsType();
		MetaFetcher fetcher = null;
		boolean isRelational = false;
		if (dsType.equalsIgnoreCase("mysql") || dsType.equalsIgnoreCase("oracle")
				){
			fetcher = MetaFetcher.getFetcher(dataSource);
			isRelational = true;
		}
		try {
			for (DataTable table : newTables) {
				TableVersion tableVersion = new TableVersion();
				tableVersion.setDbName(table.getDsName());
				tableVersion.setSchemaName(table.getSchemaName());
				tableVersion.setTableName(table.getTableName());
				tableVersion.setVersion(0);
				tableVersion.setInnerVersion(0);
				tableVersion.setUpdateTime(new Timestamp(System.currentTimeMillis()));
				tableVersion.setDsId(table.getDsId());
				tableVersion.setEventOffset(0L);
				tableVersion.setEventPos(0L);
				tableVersion.setTableId(table.getId());
				tableVersionMapper.insert(tableVersion);
				tableMapper.updateVerId(tableVersion.getId(), table.getId());
				if(isRelational){
					HashMap<String, Object> params = new HashMap<>();
					params.clear();
					params.put("schemaName", table.getSchemaName());
					params.put("tableName", table.getTableName());
					List<TableMeta> tableMetas = fetcher.fetchMeta(params);
					for (TableMeta tableMeta : tableMetas) {
						tableMeta.setVerId(tableVersion.getId());
						tableMeta.setOriginalSer(0);
						tableMetaMapper.insert(tableMeta);
					}
				}
			}
		} finally {
			if (fetcher != null) {
				fetcher.close();
			}
		}
	}

	/**
	 * 构造默认的table for MySQL
	 *
	 * @return
	 */
	public List<DataTable> getDefaultTableForMySQL(Integer dsId, String dsName, Integer schemaId) {
		List<DataTable> resultList = new ArrayList<>();
		String outputTopic = dsName + OUTPUT_TOPIC_SUFFIX_LOWER;

		DataTable hbMonitorTable = new DataTable(dsId, dsName, schemaId, DataSchemaService.DBUS,
				TABLE_HB_MONITOR_LOWER, TABLE_HB_MONITOR_LOWER, outputTopic, "ok");
		resultList.add(hbMonitorTable);

		DataTable fullPullTable = new DataTable(dsId, dsName, schemaId, DataSchemaService.DBUS,
				TABLE_FULL_PULL_LOWER, TABLE_FULL_PULL_LOWER, outputTopic, "ok");

		resultList.add(fullPullTable);

		return resultList;
	}

	/**
	 * 构造默认的table for 非MySQL
	 *
	 * @return
	 */
	public List<DataTable> getDefaultTableForNotMySQL(Integer dsId, String dsName, Integer schemaId) {

		List<DataTable> resultList = new ArrayList<>();
		String outputTopic = dsName + OUTPUT_TOPIC_SUFFIX_UPPER;

		DataTable hbMonitorTable = new DataTable(dsId, dsName, schemaId, DataSchemaService.DBUS.toUpperCase(),
				TABLE_HB_MONITOR_UPPER, TABLE_HB_MONITOR_UPPER, outputTopic, "ok");

		DataTable fullPullTable = new DataTable(dsId, dsName, schemaId, DataSchemaService.DBUS.toUpperCase(),
				TABLE_FULL_PULL_UPPER, TABLE_FULL_PULL_UPPER, outputTopic, "ok");

		DataTable metaSyncEventTable = new DataTable(dsId, dsName, schemaId, DataSchemaService.DBUS.toUpperCase(),
				TABLE_META_SYNC_EVENT, TABLE_META_SYNC_EVENT, outputTopic, "ok");

		resultList.add(hbMonitorTable);
		resultList.add(fullPullTable);
		resultList.add(metaSyncEventTable);

		return resultList;
	}

	/**
	 * 判断table是否是默认的表
	 *
	 * @return true: 是; false: 否;
	 */
	public boolean ifDefaultTable(String tableName) {
		boolean flag = false;
		if (StringUtils.equals(tableName, TABLE_FULL_PULL_LOWER)
				|| StringUtils.equals(tableName, TABLE_FULL_PULL_UPPER)
				|| StringUtils.equals(tableName, TABLE_HB_MONITOR_LOWER)
				|| StringUtils.equals(tableName, TABLE_HB_MONITOR_UPPER)
				|| StringUtils.equals(tableName, TABLE_META_SYNC_EVENT)) {
			flag = true;
		}
		return flag;
	}

	public List<HashMap<String, Object>> findTablesByUserId(Integer userId) {
		return tableMapper.findTablesByUserId(userId);
	}

	public List<DataTable> findAllTables() {
		return tableMapper.findAllTables();
	}

	public List<DataTable> findActiveTablesBySchemaId(Integer schemaId) {
		return tableMapper.findActiveTablesBySchemaId(schemaId);
	}

	public List<DataTable> findActiveTablesByDsId(Integer schemaId) {
		return tableMapper.findActiveTablesByDsId(schemaId);
	}

	public void startOrStopTableByTableIds(ArrayList<Integer> ids, String status) {
		tableMapper.startOrStopTableByTableIds(ids, status);
	}

	public List<Map<String, Object>> getDataSourcesByTableIds(ArrayList<Integer> ids) {
		return tableMapper.getDataSourcesByTableIds(ids);
	}

	public void importRulesByTableId(Integer tableId, String json) {
		JSONArray jsonArray = JSONArray.parseArray(json);
		for (int i = 0; i < jsonArray.size(); i++) {
			JSONObject groupRules = (JSONObject) jsonArray.get(i);
			DataTableRuleGroup dataTableRuleGroup = new DataTableRuleGroup();
			dataTableRuleGroup.setTableId(tableId);
			dataTableRuleGroup.setGroupName(groupRules.getString("groupName"));
			dataTableRuleGroup.setStatus("inactive");
			dataTableRuleMapper.addGroup(dataTableRuleGroup);
			JSONArray rules = groupRules.getJSONArray("rules");
			int order = 0;
			ArrayList<DataTableRule> dataTableRules = new ArrayList<>();
			for (int j = 0; j < rules.size(); j++) {
				JSONObject rule = (JSONObject) rules.get(j);
				DataTableRule dataTableRule = new DataTableRule();
				dataTableRule.setGroupId(dataTableRuleGroup.getId());
				dataTableRule.setOrderId(order);
				dataTableRule.setRuleTypeName(rule.getString("ruleTypeName"));
				dataTableRule.setRuleGrammar(rule.getString("ruleGrammar"));
				dataTableRules.add(dataTableRule);
				order++;
			}
			dataTableRuleMapper.saveAllRules(dataTableRules);
		}
	}

	public String exportRulesByTableId(Integer tableId) {
		JSONArray resultJson = new JSONArray();
		List<DataTableRuleGroup> allRuleGroup = dataTableRuleMapper.getAllRuleGroup(tableId);
		for (DataTableRuleGroup dataTableRuleGroup : allRuleGroup) {
			List<DataTableRule> allRules = dataTableRuleMapper.getAllRules(dataTableRuleGroup.getId());
			JSONObject groupRules = new JSONObject();
			groupRules.put("groupName", dataTableRuleGroup.getGroupName());
			JSONArray rules = new JSONArray();
			for (DataTableRule dataTableRule : allRules) {
				JSONObject rule = new JSONObject();
				rule.put("ruleTypeName", dataTableRule.getRuleTypeName());
				rule.put("ruleGrammar", dataTableRule.getRuleGrammar());
				rules.add(rule);
			}
			groupRules.put("rules", rules);
			resultJson.add(groupRules);
		}
		return resultJson.toJSONString();
	}

	public List<DataTable> searchTableByIds(ArrayList<Integer> ids){
		return tableMapper.searchTableByIds(ids);
	}
}
