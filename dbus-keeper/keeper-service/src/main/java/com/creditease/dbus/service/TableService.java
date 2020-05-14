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


package com.creditease.dbus.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.bean.SourceTablesBean;
import com.creditease.dbus.commons.log.processor.parse.RuleGrammar;
import com.creditease.dbus.commons.log.processor.rule.impl.Rules;
import com.creditease.dbus.constant.MessageCode;
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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

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
    private EncodeColumnsMapper desMapper;
    @Autowired
    private DataTableRuleMapper dataTableRuleMapper;
    @Autowired
    private TableVersionMapper tableVersionMapper;
    @Autowired
    private DataSourceService dataSourceService;
    @Autowired
    private DataSchemaMapper dataSchemaMapper;
    @Autowired
    private NameAliasMappingMapper nameAliasMappingMapper;
    @Autowired
    private DdlEventMapper ddlEventMapper;
    @Autowired
    private ProjectTableService projectTableService;

    private static Logger logger = LoggerFactory.getLogger(TableService.class);

    private static final String NONE = "无";

    //默认的table 名称
    public static final String TABLE_HB_MONITOR_LOWER = "db_heartbeat_monitor";
    public static final String TABLE_HB_MONITOR_UPPER = "DB_HEARTBEAT_MONITOR";
    public static final String TABLE_META_SYNC_EVENT = "META_SYNC_EVENT";

    private static final String OUTPUT_TOPIC_SUFFIX_LOWER = ".dbus.result";//构造output的后缀
    private static final String OUTPUT_TOPIC_SUFFIX_UPPER = ".DBUS.result";

    private static final int INIT_VERSION = 0;//一些版本的初始值

    public PageInfo<DataTable> getTablesInPages(Integer dsId, Integer schemaId, String schemaName, String tableName, int pageNum, int pageSize) {
        PageHelper.startPage(pageNum, pageSize);
        if (StringUtils.isBlank(schemaName)) schemaName = null;
        if (StringUtils.isBlank(tableName)) tableName = null;
        List<DataTable> tables = tableMapper.findTables(dsId, schemaId, StringUtils.trim(schemaName), StringUtils.trim(tableName));
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

    public int deleteTable(int tableId) throws Exception {
        DataTable table = getTableById(tableId);
        //删除表数据
        tableMapper.deleteByTableId(tableId);
        //oracle类型需要删除源端DBUS_TABLES对应的表
        if (table.getDsType().equalsIgnoreCase("oracle")) {
            HashMap<String, Object> params = new HashMap<>();
            params.put("dsType", table.getDsType());
            params.put("URL", table.getMasterUrl());
            params.put("user", table.getDbusUser());
            params.put("password", table.getDbusPassword());
            SourceFetcher sourceFetcher = SourceFetcher.getFetcher(params);
            this.deleteDbusTables(sourceFetcher, table);
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

    public List<EncodeColumns> getDesensitizationInfo(Integer tableId) {
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
            EncodeColumns di = new EncodeColumns();
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
         * 在t_meta_version中生成新版本号,同时更新t_data_table中的版本
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
        /**
         * 模拟表结构变更
         */
        createDdlEvent(map, verId);
        return validateResult;
    }

    private void createDdlEvent(Map<String, Object> map, int verId) {
        DdlEvent ddlEvent = new DdlEvent();
        ddlEvent.setEventId(0);
        ddlEvent.setDsId(Integer.parseInt(map.get("dsId").toString()));
        ddlEvent.setSchemaName(map.get("schemaName").toString());
        ddlEvent.setTableName(map.get("tableName").toString());
        ddlEvent.setVerId(verId);
        ddlEvent.setDdlType("ALTER");
        ddlEvent.setDdl("dbus upgrade version");
        ddlEvent.setUpdateTime(new Date());
        ddlEventMapper.insert(ddlEvent);
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
     * 获取源端表,主要包含tablenames信息
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
                    || DbusDatasourceType.stringEqual(dataSource.getDsType(), DbusDatasourceType.DB2)
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
     * 获取源端的table信息,包括Incompatible Column等信息
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
                //根据不同类型的dataSource,来选择不同的TableFetcher
                if (DbusDatasourceType.stringEqual(dataSource.getDsType(), DbusDatasourceType.MYSQL)
                        || DbusDatasourceType.stringEqual(dataSource.getDsType(), DbusDatasourceType.ORACLE)
                        || DbusDatasourceType.stringEqual(dataSource.getDsType(), DbusDatasourceType.DB2)
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


            /*构造[ {},{"dsName","schemaName","tableNames"},{} ] 的参数集合
              由于fetchTables只能获取tableName信息,没有dsName和schemaName,所以这两个需要传入
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

                //对比两个结果,已添加的表需要赋值和 disable = true;
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
     * 加表时,将表的相关信息插入源端的库
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
                //int result = isOpenSupplementalLog(sourceFetcher, tableInfos);
                //if (result != 0) {
                //	errorResult.setStatus(result);
                //	return errorResult;
                //}

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
     *
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

    public DataTable findBySchemaIdAndTableName(int schemaId, String tableName) {
        return tableMapper.findBySchemaIdTableName(schemaId, tableName);
    }


    /**
     * 管理库中插入表
     *
     * @param newTable
     * @return 根据shcemaId和tableName校验：
     * 如果存在,直接返回 0,
     * 否则,添加后返回tableId,
     * 异常 返回-1
     */
    public int insertManageTable(DataTable newTable) {
        Integer schemaId = newTable.getSchemaId();
        if (schemaId == null) {
            logger.error("[insert manage table] SchemaId is null! Table:{}", newTable);
            return -1;
        }
        String tableName = newTable.getTableName();
        //table 已存在,不添加
        DataTable table = findBySchemaIdAndTableName(schemaId, tableName);
        if (table != null) {
            newTable.setId(table.getId());
            logger.info("[insert manage table] Table already exists. schemaId:{}, tableId:{}, tableName:{}", schemaId, table.getId(), tableName);
            return 0;
        }
        //插入table
        newTable.setCreateTime(new Timestamp(System.currentTimeMillis()));
        tableMapper.insert(newTable);
        logger.info("[insert manage table] success. schemaId:{}, tableId:{}, tableName:{}", schemaId, newTable.getId(), tableName);
        return newTable.getId();
    }

    /**
     * 批量添加table
     *
     * @return 如果某个表添加出错, 跳过;
     */
    public int insertManageTable(List<DataTable> newTables) throws Exception {
        //记录失败的数量
        int failCount = 0;
        Iterator<DataTable> iterator = newTables.iterator();
        while (iterator.hasNext()) {
            DataTable dataTable = iterator.next();
            int id = insertManageTable(dataTable);
            //不合规的表跳过,记录数量
            if (id == -1) {
                failCount++;
                iterator.remove();
            }
            //为0说明存在了,不需要初始化meta,直接remove掉
            if (id == 0) {
                iterator.remove();
            }
        }
        if (newTables != null && newTables.size() > 0) {
            initTableMeta(newTables);
        }
        return failCount;
    }

    /**
     * web 初始化 meta
     *
     * @param newTables
     * @throws Exception
     */
    private void initTableMeta(List<DataTable> newTables) throws Exception {
        DataSource dataSource = dataSourceService.getById(newTables.get(0).getDsId());
        String dsType = dataSource.getDsType();
        MetaFetcher fetcher = null;
        boolean isRelational = false;
        if (dsType.equalsIgnoreCase("mysql") || dsType.equalsIgnoreCase("oracle")
                || dsType.equalsIgnoreCase("db2")
                ) {
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
                if (isRelational) {
                    HashMap<String, Object> params = new HashMap<>();
                    params.clear();
                    params.put("schemaName", table.getSchemaName());
                    params.put("tableName", table.getTableName());
                    List<TableMeta> tableMetas = null;
                    try {
                        tableMetas = fetcher.fetchMeta(params);
                    } catch (Exception e) {
                        logger.warn("获取meta详情异常,{}.{}", table.getSchemaName(), table.getTableName());
                        logger.error(e.getMessage(), e);
                        continue;
                    }
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

    public DataTable reInitTableMeta(Integer tableId) throws Exception {
        DataTable table = tableMapper.findById(tableId);
        logger.info("收到重新加载meta的请求,tableId:{},tableName:{}", tableId, table.getTableName());
        TableVersion oldVersion = tableVersionMapper.getVersionByTableId(table.getId());
        DataSource dataSource = dataSourceService.getById(table.getDsId());
        String dsType = dataSource.getDsType();
        MetaFetcher fetcher = null;
        boolean isRelational = false;
        if (dsType.equalsIgnoreCase("mysql") || dsType.equalsIgnoreCase("oracle")
                || dsType.equalsIgnoreCase("db2")
                ) {
            fetcher = MetaFetcher.getFetcher(dataSource);
            isRelational = true;
        }
        try {
            Integer version = 0;
            Integer innerVersion = 0;
            List<TableMeta> newMetas = null;
            List<TableMeta> oldMetas = null;
            Long eventOffset = 0L;
            Long eventPos = 0L;
            if (isRelational) {
                HashMap<String, Object> params = new HashMap<>();
                params.clear();
                params.put("schemaName", table.getSchemaName());
                params.put("tableName", table.getTableName());
                newMetas = fetcher.fetchMeta(params);
                if (oldVersion != null) {
                    //20190924,这里表结构不再判断,直接版本号+1
                    //手动升级表版本号
                    //oldMetas = tableMetaMapper.selectByTableId(table.getId());
                    //Integer compareResult = this.compareMeta(oldMetas, newMetas);
                    Integer compareResult = 1;
                    if (compareResult == 1) {
                        logger.info("表结构不兼容变更,需要升级版本,版本号 + 1 .currentVersion:{}", oldVersion.getVersion());
                        version = oldVersion.getVersion() + 1;
                        innerVersion = oldVersion.getInnerVersion() + 1;
                        eventOffset = oldVersion.getEventOffset() + 1;
                        eventPos = oldVersion.getEventPos() + 1;
                    } else if (compareResult == 0) {
                        version = oldVersion.getVersion();
                        innerVersion = oldVersion.getInnerVersion();
                        eventOffset = oldVersion.getEventOffset();
                        eventPos = oldVersion.getEventPos();
                        logger.info("表结构兼容性变更 .currentVersion:{}", oldVersion.getVersion());
                    } else {
                        logger.info("表结构未发生变更,无需更新 .currentVersion:{}", oldVersion.getVersion());
                        return table;
                    }
                }
            }
            TableVersion tableVersion = new TableVersion();
            tableVersion.setDbName(table.getDsName());
            tableVersion.setSchemaName(table.getSchemaName());
            tableVersion.setTableName(table.getTableName());
            tableVersion.setVersion(version);
            tableVersion.setInnerVersion(innerVersion);
            tableVersion.setUpdateTime(new Timestamp(System.currentTimeMillis()));
            tableVersion.setDsId(table.getDsId());
            tableVersion.setEventOffset(eventOffset);
            tableVersion.setEventPos(eventPos);
            tableVersion.setTableId(table.getId());
            tableVersionMapper.insert(tableVersion);
            tableMapper.updateVerId(tableVersion.getId(), table.getId());

            //新增ddlevent数据
            DdlEvent ddlEvent = new DdlEvent();
            ddlEvent.setEventId(0);
            ddlEvent.setDsId(table.getDsId());
            ddlEvent.setSchemaName(table.getSchemaName());
            ddlEvent.setTableName(table.getTableName());
            ddlEvent.setVerId(tableVersion.getId());
            ddlEvent.setDdlType("ALTER");
            ddlEvent.setDdl("dbus upgrade version");
            ddlEvent.setUpdateTime(new Date());
            ddlEventMapper.insert(ddlEvent);

            if (isRelational) {
                for (TableMeta tableMeta : newMetas) {
                    tableMeta.setVerId(tableVersion.getId());
                    tableMeta.setOriginalSer(innerVersion);
                    tableMetaMapper.insert(tableMeta);
                }
            }
            return table;
        } finally {
            if (fetcher != null) {
                fetcher.close();
            }
        }
    }

    /**
     * 这里仅仅简单的比较列数量/名称/类型,其他不做比较
     *
     * @param oldMetas
     * @param newMetas
     * @return
     */
    private Integer compareMeta(List<TableMeta> oldMetas, List<TableMeta> newMetas) {
        if (oldMetas.size() != newMetas.size()) {
            //不兼容
            return 1;
        }
        Map<String, TableMeta> oldMetaMap = oldMetas.stream().collect(Collectors.toMap(TableMeta::getColumnName, meta -> meta));
        for (TableMeta newMeta : newMetas) {
            String columnName = newMeta.getColumnName();
            String dataType = newMeta.getDataType();
            TableMeta oldMeta = oldMetaMap.get(columnName);
            if (StringUtils.isBlank(dataType)) {
                //不兼容
                return 1;
            }
            if (!dataType.equals(oldMeta.getDataType())
                    || !newMeta.getDataLength().equals(oldMeta.getDataLength())
                    || newMeta.getDataPrecision() != oldMeta.getDataPrecision()
                    || newMeta.getDataScale() != oldMeta.getDataScale()) {
                //兼容性变更
                return 0;
            }
        }
        return -1;
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

        DataTable metaSyncEventTable = new DataTable(dsId, dsName, schemaId, DataSchemaService.DBUS.toUpperCase(),
                TABLE_META_SYNC_EVENT, TABLE_META_SYNC_EVENT, outputTopic, "ok");

        resultList.add(hbMonitorTable);
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
        if (StringUtils.equals(tableName, TABLE_HB_MONITOR_LOWER)
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

    public List<DataTable> searchTableByIds(ArrayList<Integer> ids) {
        return tableMapper.searchTableByIds(ids);
    }

    public Long getTableRowsNum(Integer id) throws Exception {
        DataTable table = getTableById(id);
        DataSource dataSource = new DataSource();
        dataSource.setId(table.getDsId());
        dataSource.setDsType(table.getDsType());
        dataSource.setMasterUrl(table.getMasterUrl());
        dataSource.setSlaveUrl(table.getSlaveUrl());
        dataSource.setDbusUser(table.getDbusUser());
        dataSource.setDbusPwd(table.getDbusPassword());
        TableFetcher fetcher = TableFetcher.getFetcher(dataSource);
        return fetcher.fetchTableDataRows(table.getSchemaName(), table.getTableName());
    }

    public String getTableRows(Integer id) throws Exception {
        DataTable table = getTableById(id);
        DataSource dataSource = new DataSource();
        dataSource.setId(table.getDsId());
        dataSource.setDsType(table.getDsType());
        dataSource.setMasterUrl(table.getMasterUrl());
        dataSource.setSlaveUrl(table.getSlaveUrl());
        dataSource.setDbusUser(table.getDbusUser());
        dataSource.setDbusPwd(table.getDbusPassword());
        TableFetcher fetcher = TableFetcher.getFetcher(dataSource);
        Long rownum = fetcher.fetchTableDataRows(table.getSchemaName(), table.getTableName());
        int length = rownum.toString().length();
        String unit = null;
        switch (length) {
            case 4:
                unit = "千";
                break;
            case 5:
                unit = "万";
                break;
            case 6:
                unit = "十万";
                break;
            case 7:
                unit = "百万";
                break;
            case 8:
                unit = "千万";
                break;
            case 9:
                unit = "亿";
                break;
            case 10:
                unit = "十亿";
                break;
            case 11:
                unit = "百亿";
                break;
            case 12:
                unit = "千亿";
                break;
            default:
                break;
        }
        if (unit == null) {
            return rownum.toString();
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append(rownum.toString().substring(0, 1)).append(unit).append(" (").append(rownum).append(")");
            return sb.toString();
        }
    }

    public int moveSourceTables(Map<String, Object> param) {
        //目标数据线
        Integer targetDsId = Integer.parseInt(param.get("dsId").toString());
        List<Integer> srcTableIds = (List<Integer>) param.get("tableIds");

        DataSource targetDataSource = dataSourceService.getById(targetDsId);
        NameAliasMapping nameAliasMapping = nameAliasMappingMapper.selectByNameId(NameAliasMapping.datasourceType, targetDsId);
        String targetDsNameAlias = nameAliasMapping == null ? targetDataSource.getDsName() : nameAliasMapping.getAlias();

        DataTable srcDataTable = tableMapper.findById(srcTableIds.get(0));
        NameAliasMapping srcNameAliasMapping = nameAliasMappingMapper.selectByNameId(NameAliasMapping.datasourceType, srcDataTable.getDsId());
        String srcDsNameAlias = srcNameAliasMapping == null ? srcDataTable.getDsName() : srcNameAliasMapping.getAlias();
        //这里必须保证目标数据源已经配置了别名,并且别名必须和源数据线保持一致
        if (!srcDsNameAlias.equals(targetDsNameAlias)) {
            return MessageCode.TARGET_DATASOURCE_HAVE_NO_ALIAS;
        }

        String targetDsName = targetDataSource.getDsName();
        String srcSchemaName = srcDataTable.getSchemaName();

        List<DataSchema> targetDataSchemas = dataSchemaMapper.searchSchema(null, targetDsId, srcSchemaName);
        DataSchema targetDataSchema = targetDataSchemas == null || targetDataSchemas.size() == 0 ? null : targetDataSchemas.get(0);
        String outputTopic = String.format("%s.%s.result", targetDsName, srcSchemaName);
        //不存在自动创建
        if (targetDataSchema == null) {
            targetDataSchema = new DataSchema();
            targetDataSchema.setDsId(targetDsId);
            targetDataSchema.setSchemaName(srcDataTable.getSchemaName());
            targetDataSchema.setStatus(DataSchema.ACTIVE);
            targetDataSchema.setSrcTopic(String.format("%s.%s", targetDsName, srcSchemaName));
            targetDataSchema.setTargetTopic(outputTopic);
            targetDataSchema.setCreateTime(new Date());
            targetDataSchema.setDescription("表迁移自动创建");
            dataSchemaMapper.insert(targetDataSchema);
        }

        //更新t_data_tables表
        tableMapper.updateByTableIds(targetDsId, targetDataSchema.getId(), srcSchemaName, outputTopic, srcTableIds);
        //更新t_meta_version
        tableVersionMapper.updateByTableIds(targetDsId, targetDsName, srcSchemaName, srcTableIds);
        return 0;
    }

    public List<DataTable> getTablesByDsId(Integer dsId) {
        return tableMapper.findTables(dsId, null, null, null);
    }

    public ResultEntity batchDeleteTableByTableIds(List<Integer> tableIds) throws Exception {
        ResultEntity resultEntity = new ResultEntity();
        for (Integer tableId : tableIds) {
            int i = projectTableService.countByTableId(tableId);
            if (i > 0) {
                resultEntity.setStatus(MessageCode.EXCEPTION);
                resultEntity.setMessage(String.format("表%s仍有项目在使用不能删除!", tableId));
                return resultEntity;
            }
        }
        for (Integer tableId : tableIds) {
            deleteTable(tableId);
        }
        return resultEntity;
    }
}
