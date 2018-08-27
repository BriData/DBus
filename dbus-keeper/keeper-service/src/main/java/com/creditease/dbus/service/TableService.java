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
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.bean.SourceTablesBean;
import com.creditease.dbus.commons.log.processor.parse.RuleGrammar;
import com.creditease.dbus.commons.log.processor.rule.impl.Rules;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.constant.ServiceNames;
import com.creditease.dbus.domain.mapper.*;
import com.creditease.dbus.domain.model.*;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.service.table.MongoTableFetcher;
import com.creditease.dbus.service.table.TableFetcher;
import com.fasterxml.jackson.core.type.TypeReference;
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

    private static final String NONE ="无";

    //默认的table 名称
    public static final String TABLE_FULL_PULL_LOWER = "db_full_pull_requests";
    public static final String TABLE_HB_MONITOR_LOWER =  "db_heartbeat_monitor";
    public static final String TABLE_HB_MONITOR_UPPER = "DB_HEARTBEAT_MONITOR";
    public static final String TABLE_META_SYNC_EVENT = "META_SYNC_EVENT";
    public static final String TABLE_FULL_PULL_UPPER = "DB_FULL_PULL_REQUESTS";


    private static final String OUTPUT_TOPIC_SUFFIX_LOWER = ".dbus.result";//构造output的后缀
    private static final String OUTPUT_TOPIC_SUFFIX_UPPER = ".DBUS.result";

    private static final int INIT_VERSION = 0;//一些版本的初始值

    public List<DataTable> getTables(Integer dsId, String schemaName, String tableName) {
        return tableMapper.findTables(dsId, schemaName, tableName);
    }

    public PageInfo<DataTable> getTablesInPages(Integer dsId, String schemaName, String tableName, int pageNum, int pageSize) {
        PageHelper.startPage(pageNum, pageSize);
        List<DataTable> tables = tableMapper.findTables(dsId, schemaName, tableName);
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

    public int deleteTable(int tableId) {
        return tableMapper.deleteByTableId(tableId);
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
        // 生成新的规则组信息
        DataTableRuleGroup group = new DataTableRuleGroup();
        group.setTableId(tableId);
        group.setGroupName(map.get("newName").toString());
        group.setStatus(map.get("newStatus").toString());

        int insertResult = dataTableRuleMapper.addGroup(group);
        if (insertResult == 0) {
            throw new SQLException();
        }
        List<DataTableRule> rules = dataTableRuleMapper.getAllRules(tableId);
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
            tv.setEventOffset(0);
            tv.setEventPos(0);
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
            tableMeta.setVerId((long) verId);
            tableMeta.setOriginalColumnName(ruleGrammar.getName());
            tableMeta.setColumnName(ruleGrammar.getName());
            tableMeta.setColumnId(Integer.parseInt(ruleGrammar.getRuleScope()));
            tableMeta.setInternalColumnId(Integer.parseInt(ruleGrammar.getRuleScope()));
            tableMeta.setOriginalSer(0L);
            tableMeta.setDataType(ruleGrammar.getType());
            tableMeta.setDataLength(0);
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

    public List<DataTable> getSchemaTables(int dsId, String schemaName){
        return tableMapper.findByDsIdAndSchemaName(dsId,schemaName);
    }

    /**
     * 获取源端表，主要包含tablenames信息
     *
     * @return list: 正确结果 | null: 异常 | Exception：参数类型不对
     */
    private List<DataTable> fetchTables(int dsId,String schemaName) throws Exception{
        try {
            DataSource dataSource = dataSourceService.getById(dsId);
            String dsName=dataSource.getDsName();
            List<DataTable> list;

            if(DbusDatasourceType.stringEqual(dataSource.getDsType(),DbusDatasourceType.MYSQL)
                    || DbusDatasourceType.stringEqual(dataSource.getDsType(),DbusDatasourceType.ORACLE))
            {
                TableFetcher fetcher = TableFetcher.getFetcher(dataSource);
                Map<String, Object> map = new HashMap<>();
                map.put("dsName", dsName);
                map.put("schemaName", schemaName);
                list = fetcher.fetchTable(map);
            } else if(DbusDatasourceType.stringEqual(dataSource.getDsType(),DbusDatasourceType.MONGO)) {
                MongoTableFetcher fetcher = new MongoTableFetcher(dataSource);
                list = fetcher.fetchTable(schemaName);
            } else {
                throw new IllegalArgumentException("Unsupported datasource type");
            }

            return list;
        } catch (Exception e) {
            logger.error("[fetch tables] dsId:{}, schemaName: {}, Catch exception: {}",dsId,schemaName,e);
            throw e;
        }
    }

    /**
     * 获取源端的table信息，包括Incompatible Column等信息
     * @param paramsList
     * @return
     */
    private List<List<TableMeta>> fetchTableField(List<Map<String, Object>> paramsList) throws Exception{
        List<List<TableMeta>> ret = new ArrayList<>();

        for (Map<String, Object> params : paramsList) {
            try {
                String dsName = String.valueOf(params.get("dsName"));

                List<DataSource> dataSourceList = dataSourceService.getDataSourceByName(dsName);
                if(dataSourceList.size() <0){
                    // return
                    logger.error("[Fetch table field] table not found. dsName:{}",dsName);
                }
                DataSource dataSource = dataSourceList.get(0);
                //根据不同类型的dataSource，来选择不同的TableFetcher
                if(DbusDatasourceType.stringEqual(dataSource.getDsType(), DbusDatasourceType.MYSQL)
                        || DbusDatasourceType.stringEqual(dataSource.getDsType(), DbusDatasourceType.ORACLE)) {
                    TableFetcher fetcher = TableFetcher.getFetcher(dataSource);
                    ret = fetcher.fetchTableField(params, paramsList);

                } else if(DbusDatasourceType.stringEqual(dataSource.getDsType(), DbusDatasourceType.MONGO)){
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

    public List<Map<String,Object>> tableList(int dsId, String dsName,String schemaName) throws Exception{
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
            List<Map<String,Object>> resultTableInfo = new ArrayList<>();

            for(int i = 0; i < sourceTables.size(); i++)
            {
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
                for( int j = 0; j < managerTables.size(); j++) {
                    if(StringUtils.equals(sourceTableName, managerTables.get(j).getTableName()))
                    {
                        //__ckbox_checked__ = true;
                        disable = true;
                        physicalTableRegex = managerTables.get(j).getPhysicalTableRegex();
                        outputTopic = managerTables.get(j).getOutputTopic();
                        break;
                    }
                }

                if(metaLength > 0) {
                    for(TableMeta index : meta) {
                        ignoreColumnName += index.getColumnName() + '/' + index.getDataType() + ' ';
                        if(index.getIncompatibleColumn() !=null)
                        {
                            incompatibleColumn += index.getIncompatibleColumn() + ' ';
                        }
                    }
                    if (StringUtils.isEmpty(incompatibleColumn))
                        incompatibleColumn = NONE;

                    Map<String,Object> resultTableMap = new HashedMap();
                    resultTableMap.put("tableName",sourceTables.get(i).getTableName());
                    resultTableMap.put("physicalTableRegex",physicalTableRegex);
                    resultTableMap.put("outputTopic",outputTopic);
                    resultTableMap.put("columnName",ignoreColumnName);
                    resultTableMap.put("incompatibleColumn",incompatibleColumn);
                    resultTableMap.put("disable",disable);

                    resultTableInfo.add(resultTableMap);
                }
                else {
                    Map<String,Object> resultTableMap = new HashedMap();
                    resultTableMap.put("tableName",sourceTables.get(i).getTableName());
                    resultTableMap.put("physicalTableRegex",physicalTableRegex);
                    resultTableMap.put("outputTopic",outputTopic);
                    resultTableMap.put("columnName",NONE);
                    resultTableMap.put("incompatibleColumn",NONE);
                    resultTableMap.put("disable",disable);

                    resultTableInfo.add(resultTableMap);

                }
            }
            return  resultTableInfo;
        }catch (Exception e){
            logger.error("[tables to add] Exception: ",e);
            throw e;
        }


    }

    public DataTable getByDsSchemaTableName(String dsName, String schemaName, String tableName) {
        return tableMapper.getByDsSchemaTableName(dsName,schemaName,tableName);
    }

    /**
     * 加表时，将表的相关信息插入源端的库
     */
    public ResultEntity insertTableInSource(SourceTablesBean sourceTablesInfo){
        int dsId = sourceTablesInfo.getDsId();
        List<DataTable> sourceTables = sourceTablesInfo.getSourceTables();
        //String sourceTables = String.valueOf(param.get("sourceTables"));
        return insertTableInSource(dsId,sourceTables);
    }

    private ResultEntity insertTableInSource(int dsId,List<DataTable> tableInfos){
        try{
            DataSource ds =dataSourceService.getById(dsId);
            String dsType = ds.getDsType();

            if(tableInfos.size() >0 && !StringUtils.equals(dsType,"mysql")){
                //获取已添加列表
                List<DataTable> sourceTableList = getSourceTableList(dsId);

                //在插入操作前，先进行数据格式判断
                for(DataTable table: sourceTableList){
                    if(StringUtils.isEmpty(table.getSchemaName()) ||
                            StringUtils.isEmpty(table.getTableName())){
                        logger.error("[insert source table]Param error. schemaName:{},tableName{}",
                                table.getSchemaName(),table.getTableName());
                        ResultEntity errorResult =new ResultEntity();
                        errorResult.setStatus(MessageCode.SOURCE_TABLE_INSERT_ERROR);
                        return errorResult;
                    }
                }

                for(int i=0;i<tableInfos.size(); i++){
                    String schemaName = tableInfos.get(i).getSchemaName();
                    String tableName = tableInfos.get(i).getTableName();

                    for(DataTable oldTable: sourceTableList){
                        //已存在的不添加
                        if(StringUtils.equals(schemaName,oldTable.getSchemaName())
                                && StringUtils.equals(tableName,oldTable.getTableName())){
                            continue;
                        }else {
                        //源表没有，添加
                            int insertResult =insertSourceTable(dsId,schemaName,tableName);
                        }
                    }

                }

            }
            return new ResultEntity();

        }catch (Exception e){
            logger.error("[insert table in resource]Exception: {}",e);
            ResultEntity errorResult =new ResultEntity();
            errorResult.setStatus(MessageCode.SOURCE_TABLE_INSERT_ERROR);
            errorResult.setMessage(e.getMessage());
            return errorResult;
        }

    }

    /**
     * 获取源端表中已添加的表的信息
     */
    private List<DataTable> getSourceTableList(int dsId) {
        try {
            DataSource ds =dataSourceService.getById(dsId);
            String dsType = ds.getDsType();
            if("mysql".equals(dsType))
                return new ArrayList<>(0);
            TableFetcher fetcher = TableFetcher.getFetcher(ds);
            List<DataTable> list = fetcher.listTable();
            return list;
        } catch (Exception e) {
            logger.error("[source table list] Exception: {}",e);
            return null;
        }

    }

    /**
     * 往源端表中插入新的table信息：最后插入的步骤
     * @throws Exception
     */
    private int insertSourceTable(int dsId,String schemaName, String tableName) throws Exception{
        try {
            DataSource ds = dataSourceService.getById(dsId);
            String dsType = ds.getDsType();
            if("mysql".equals(dsType))
                return 0;

            TableFetcher fetcher = TableFetcher.getFetcher(ds);
            Map<String, Object> map = new HashMap<>();
            map.put("schemaName",schemaName);
            map.put("tableName",tableName);
            int  t = fetcher.insertTable(map);
            return t;
        } catch (Exception e) {
            logger.error("[insert source table] Exception: {}",e);
            throw e;
        }
    }

    private DataTable findBySchemaIdAndTableName(int schemaId, String tableName){
        return tableMapper.findBySchemaIdTableName(schemaId,tableName);
    }


    /**
     * 管理库中插入表
     * @param newTable
     * @return 根据shcemaId和tableName校验：
     *      如果存在，直接返回 0；
     *      否则，添加后返回tableId；
     *      异常 返回-1
     */
    public int insertManageTable(DataTable newTable){
        Integer schemaId = newTable.getSchemaId();
        if(schemaId == null){
            logger.error("[insert manage table] SchemaId is null! Table:{}",newTable);
            return -1;
        }
        String tableName = newTable.getTableName();
        //table 已存在，不添加
        if(findBySchemaIdAndTableName(schemaId,tableName) != null){
            logger.info("[insert manage table] Table already exists. schemaId:{},  tableName:{}",schemaId,tableName);
            return 0;
        }
        //插入table
        newTable.setCreateTime(new Timestamp(System.currentTimeMillis()));
        tableMapper.insert(newTable);

        //t_data_tables中没有dsName等信息，查询后可获得 不插入。
        newTable = getById(newTable.getId());
        DbusDatasourceType dsType = DbusDatasourceType.parse(newTable.getDsType());
        //此处默认，非MySQL和Oracle的则为log类型，需要处理version信息;MySQL和Oracle的meta和version信息由增量处理
        if(DbusDatasourceType.MYSQL != dsType && DbusDatasourceType.ORACLE != dsType){
            //插入table后，将table的version放入t_meta_verion中
            TableVersion tableVersion = new TableVersion();
            tableVersion.setTableId(newTable.getId());
            tableVersion.setDsId(newTable.getDsId());
            tableVersion.setDbName(newTable.getDsName());
            tableVersion.setSchemaName(newTable.getSchemaName());
            tableVersion.setTableName(newTable.getTableName());
            tableVersion.setVersion(INIT_VERSION);
            tableVersion.setInnerVersion(INIT_VERSION);
            tableVersion.setEventOffset(INIT_VERSION);
            tableVersion.setEventPos(INIT_VERSION);
            tableVersionMapper.insert(tableVersion);

            //meta插入完毕后，更新table的verId
            newTable.setVerId(tableVersion.getId());
            tableMapper.updateByPrimaryKey(newTable);
        }

        return newTable.getId();
    }

    /**
     * 批量添加table
     * @return 如果某个表添加出错，直接return -1;
     */
    public int insertManageTable(List<DataTable> newTables){
        for(DataTable table:newTables){
            if(insertManageTable(table) == -1){
                return -1;
            }
        }
        return 0;
    }

    /**
     * 构造默认的table for MySQL
     * @return
     */
    public List<DataTable> getDefaultTableForMySQL(Integer dsId, String dsName,Integer schemaId){

        List<DataTable> resultList = new ArrayList<>();
        String outputTopic = dsName+OUTPUT_TOPIC_SUFFIX_LOWER;

        DataTable hbMonitorTable = new DataTable(dsId,schemaId,DataSchemaService.DBUS,
                TABLE_HB_MONITOR_LOWER,TABLE_HB_MONITOR_LOWER,outputTopic,"abort");
        resultList.add(hbMonitorTable);

        DataTable fullPullTable = new DataTable(dsId,schemaId,DataSchemaService.DBUS,
                TABLE_FULL_PULL_LOWER,TABLE_FULL_PULL_LOWER,outputTopic,"abort");

        resultList.add(fullPullTable);

        return resultList;
    }

    /**
     * 构造默认的table for 非MySQL
     * @return
     */
    public List<DataTable> getDefaultTableForNotMySQL(Integer dsId, String dsName,Integer schemaId){

        List<DataTable> resultList = new ArrayList<>();
        String outputTopic = dsName+OUTPUT_TOPIC_SUFFIX_UPPER;

        DataTable hbMonitorTable = new DataTable(dsId,schemaId,DataSchemaService.DBUS.toUpperCase(),
                TABLE_HB_MONITOR_UPPER,TABLE_HB_MONITOR_UPPER,outputTopic,"abort");

        DataTable fullPullTable = new DataTable(dsId,schemaId,DataSchemaService.DBUS.toUpperCase(),
                TABLE_FULL_PULL_UPPER,TABLE_FULL_PULL_UPPER,outputTopic,"abort");

        DataTable metaSyncEventTable = new DataTable(dsId,schemaId,DataSchemaService.DBUS.toUpperCase(),
                TABLE_META_SYNC_EVENT,TABLE_META_SYNC_EVENT,outputTopic,"abort");

        resultList.add(hbMonitorTable);
        resultList.add(fullPullTable);
        resultList.add(metaSyncEventTable);

        return resultList;
    }

    /**
     * 判断table是否是默认的表
     * @return true: 是; false: 否;
     */
    public boolean ifDefaultTable(String tableName){
        boolean flag = false;
        if(StringUtils.equals(tableName,TABLE_FULL_PULL_LOWER)
                || StringUtils.equals(tableName,TABLE_FULL_PULL_UPPER)
                || StringUtils.equals(tableName,TABLE_HB_MONITOR_LOWER)
                || StringUtils.equals(tableName,TABLE_HB_MONITOR_UPPER)
                || StringUtils.equals(tableName,TABLE_META_SYNC_EVENT)){
            flag = true;
        }
        return flag;
    }


    public List<DataTable> findAllTables() {
        return tableMapper.findAllTables();
    }
}
