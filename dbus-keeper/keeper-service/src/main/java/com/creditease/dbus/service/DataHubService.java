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
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.bean.DataBaseConnectInfoBean;
import com.creditease.dbus.bean.DataSourceBean;
import com.creditease.dbus.bean.DataTableBean;
import com.creditease.dbus.bean.DataTableMetaBean;
import com.creditease.dbus.domain.mapper.*;
import com.creditease.dbus.domain.model.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class DataHubService {

    private static Logger logger = LoggerFactory.getLogger(DataHubService.class);

    @Autowired
    private DataSourceMapper dataSourceMapper;
    @Autowired
    private DataSchemaMapper dataSchemaMapper;
    @Autowired
    private DatahubTableMapper dataTableMapper;
    @Autowired
    private TableVersionMapper tableVersionMapper;
    @Autowired
    private ToolSetService toolSetService;
    @Autowired
    private TableMetaMapper tableMetaMapper;
    @Autowired
    private DdlEventMapper ddlEventMapper;
    @Autowired
    private NameAliasMappingMapper nameAliasMappingMapper;
    @Autowired
    private TableService tableService;

    private static final Pattern IP_PATTERN = Pattern.compile("(2(5[0-5]{1}|[0-4]\\d{1})|[0-1]?\\d{1,2})(\\.(2(5[0-5]{1}|[0-4]\\d{1})|[0-1]?\\d{1,2})){3}");
    private static final Pattern MYSQL_URL_PATTERN = Pattern.compile("jdbc:mysql://([\\w-\\d\\\\.]+):(\\d+)/.*", Pattern.CASE_INSENSITIVE);
    private static final Pattern ORACLE_URL_PATTERN = Pattern.compile("HOST\\s*=\\s*([\\w-\\d\\\\.]+)\\)\\(PORT\\s*=\\s*(\\d+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern ORACLE_SERVICE_PATTERN = Pattern.compile("SERVICE_NAME\\s*=\\s*([\\w-\\d\\\\.]+)\\)", Pattern.CASE_INSENSITIVE);

    public ResultEntity getDBusTableInfo(HashMap<String, Object> param) throws Exception {
        Integer schemaId = (Integer) param.get("schemaId");
        ArrayList<String> tableNameList = (ArrayList<String>) param.get("tableNameList");
        return getDBusTableInfo(schemaId, tableNameList);
    }

    public List<DataTableBean> getDBusSourceInfo(String dsName, String dbName) throws Exception {
        HashMap<String, Object> param = new HashMap<>();
        param.put("dsName", dsName);
        param.put("schemaName", dbName);
        List<DataTableBean> tables = dataTableMapper.searchTablesByParam(param);
        if (tables == null || tables.size() <= 0) {
            return new ArrayList<>();
        }
        DataTableBean dataTable = tables.get(0);
        String dsType = dataTable.getDsType();
        String masterDbAddress = null;
        String slaveDbAddress = null;
        if (dsType.equals("mysql") || dsType.equals("oracle")) {
            masterDbAddress = getDbAddress(dataTable.getMasterUrl(), dataTable.getDsType());
            slaveDbAddress = getDbAddress(dataTable.getSlaveUrl(), dataTable.getDsType());
        } else {
            masterDbAddress = "empty";
            slaveDbAddress = "empty";
        }
        // 获取dbus原始的连接串
        String oriMasterUrl = dataTable.getMasterUrl();
        String oriSlaveUrl = dataTable.getSlaveUrl();
        for (DataTableBean table : tables) {
            table.setOriMasterUrl(oriMasterUrl);
            table.setOriSlaveUrl(oriSlaveUrl);

            table.setMasterUrl(masterDbAddress);
            table.setSlaveUrl(slaveDbAddress);
            List<TableMeta> tableMetaList = tableMetaMapper.selectByTableId(table.getId());
            table.setTableMetaList(tableMetaList);
        }
        return tables;
    }

    public List<DataTableBean> getDBusTableStatusBySchemaIdAndTableNames(Map<Long, List<String>> param) {
        ArrayList<DataTableBean> dataTables = new ArrayList<>();
        param.forEach((key, value) -> {
            HashMap<String, Object> params = new HashMap<>();
            params.put("schemaId", key);
            params.put("tableNames", value);
            List<DataTableBean> dataTableList = dataTableMapper.searchTablesByParam(params);
            ArrayList<DataTableBean> DataTableBeans = new ArrayList<>();
            for (DataTableBean dataTable : dataTableList) {
                DataTableBeans.add(dataTable);
            }
            dataTables.addAll(DataTableBeans);
        });
        return dataTables;
    }

    public List<TableMeta> getDBusTableFields(Integer tableId) {
        return tableMetaMapper.selectByTableId(tableId);
    }

    public List<DataTableMetaBean> getDdlDBusTableFields(String startTime, String endTime, Integer dbusId, String dbusTableName) {
        //查询全部
        List<DataTableMetaBean> dataTableMetaBeans = new ArrayList<>();
        List<DataTableBean> tables = dataTableMapper.getDdlDBusTables(startTime, endTime, dbusId, dbusTableName);
        List<DdlEvent> ddlEventlist = ddlEventMapper.selectByUpdateTime(startTime, endTime, dbusId, dbusTableName);

        tables.forEach(dataTable -> {
            Integer dsId = dataTable.getDsId();
            String schemaName = dataTable.getSchemaName();
            String tableName = dataTable.getTableName();
            ArrayList<DdlEvent> ddlEvents = new ArrayList<>();
            ddlEventlist.forEach(ddlEvent -> {
                if (ddlEvent.getDsId().equals(dsId) && ddlEvent.getSchemaName().equals(schemaName) && ddlEvent.getTableName().equals(tableName)) {
                    ddlEvents.add(ddlEvent);
                }
            });
            List<TableMeta> tableMetaList = tableMetaMapper.selectByTableId(dataTable.getId());
            DataTableMetaBean dataTableMetaBean = new DataTableMetaBean();
            dataTableMetaBean.setDataTable(dataTable);
            dataTableMetaBean.setTableMetas(tableMetaList);
            dataTableMetaBean.setDdlEvents(ddlEvents);
            dataTableMetaBeans.add(dataTableMetaBean);
        });

        return dataTableMetaBeans;
    }

    public DataBaseConnectInfoBean getDbConnectInfoByDbId(Long dbId) throws Exception {
        DataSource dataSource = dataSourceMapper.getBySchemaId(dbId.intValue());
        String dbAddress = getDbAddress(dataSource.getSlaveUrl(), dataSource.getDsType());
        String[] ipPort = dbAddress.split(",")[0].split(":");
        DataBaseConnectInfoBean connectInfoBean = new DataBaseConnectInfoBean();
        connectInfoBean.setIp(ipPort[0]);
        connectInfoBean.setPort(ipPort[1]);
        if (dataSource.getDsType().equalsIgnoreCase("oracle")) {
            connectInfoBean.setServiceName(ipPort[2]);
        }
        connectInfoBean.setDbType(dataSource.getDsType());
        connectInfoBean.setUserName(dataSource.getDbusUser());
        connectInfoBean.setPassword(dataSource.getDbusPwd());
        return connectInfoBean;
    }

    private ResultEntity getDBusTableInfo(Integer schemaId, ArrayList<String> tableNameList) throws Exception {
        ResultEntity resultEntity = new ResultEntity();
        HashMap<String, Object> param = new HashMap<>();
        param.put("schemaId", schemaId);
        param.put("tableNames", tableNameList);
        List<DataTableBean> dataTableList = dataTableMapper.searchTablesByParam(param);

        DataTableBean table = dataTableList.get(0);
        String dsName = table.getDsName();
        Integer dsId = table.getDsId();
        String dsType = table.getDsType();
        NameAliasMapping nameAliasMapping = nameAliasMappingMapper.selectByNameId(NameAliasMapping.datasourceType, dsId);
        if (nameAliasMapping != null) {
            dsName = nameAliasMapping.getAlias();
        }
        for (DataTableBean dataTableBean : dataTableList) {
            String namespace = String.format("%s.%s.%s.%s.%s.%s.%s", dsType, dsName, dataTableBean.getSchemaName(),
                    dataTableBean.getTableName(), "*", "*", "*");
            dataTableBean.setNamespace(namespace);
            if (dsType.equals("mysql") || dsType.equals("oracle")) {
                DataTable dataTable = new DataTable();
                BeanUtils.copyProperties(dataTableBean, dataTable);
                String uniqueColumn = toolSetService.getUniqueColumn(dataTable);
                logger.info("table {}.{} namespace {} ,unique column {}", dataTableBean.getSchemaName(), dataTableBean.getTableName(), namespace, uniqueColumn);
                dataTableBean.setUniqueColumn(uniqueColumn);
            }
            dataTableBean.setVersionList(tableVersionMapper.getVersionListByTableId(dataTableBean.getId()));
            dataTableBean.setDsName(dsName);
        }
        resultEntity.setPayload(JSON.toJSONString(dataTableList));
        return resultEntity;
    }

    public String getIpByHostName(String host) throws Exception {
        if (StringUtils.isBlank(host)) {
            return null;
        }
        Matcher ipMatcher = IP_PATTERN.matcher(host);
        if (!ipMatcher.matches()) {
            try {
                InetAddress address = InetAddress.getByName(host);
                host = address.getHostAddress();
            } catch (Exception e) {
                logger.error("getDBusDataSourceInfo getIpByHostName", e);
                throw e;
            }
        }
        return host;
    }

    private String getDbAddress(String url, String dsType) throws Exception {
        Matcher matcher;
        Matcher matcherServiceName = null;
        String host;
        String port;
        String result = "";
        String serviceName = null;
        if (dsType.equalsIgnoreCase("oracle")) {
            matcher = ORACLE_URL_PATTERN.matcher(url);
            matcherServiceName = ORACLE_SERVICE_PATTERN.matcher(url);
            if (matcherServiceName.find()) {
                serviceName = matcherServiceName.group(1);
            }
        } else {
            matcher = MYSQL_URL_PATTERN.matcher(StringUtils.lowerCase(url));
        }
        while (matcher.find()) {
            host = matcher.group(1);
            port = matcher.group(2);
            if (StringUtils.isNotBlank(result)) {
                result = result + "," + host + ":" + port;
            } else {
                result = host + ":" + port;
            }
            if (serviceName != null) {
                result = result + ":" + matcherServiceName.group(1);
            }
        }
        return result;
    }

    public List<DataSourceBean> getAllDataSourceInfo(String dsName) {
        List<DataSource> dataSources = dataSourceMapper.selectAll();
        List<DataSource> dataSourceList = null;
        if (StringUtils.isNotBlank(dsName)) {
            dataSourceList = dataSources.stream().filter(dataSource -> dataSource.getDsName().equals(dsName)).collect(Collectors.toList());
        } else {
            dataSourceList = dataSources.stream().filter(dataSource -> dataSource.getDsType().equals("mysql") || dataSource.getDsType().equals("oracle"))
                    .collect(Collectors.toList());
        }
        List<DataSourceBean> result = new ArrayList<>();
        dataSourceList.forEach(dataSource -> {
            DataSourceBean dataSourceBean = new DataSourceBean();
            dataSourceBean.setId(dataSource.getId());
            dataSourceBean.setDsName(dataSource.getDsName());
            dataSourceBean.setUser(dataSource.getDbusUser());
            dataSourceBean.setPass(dataSource.getDbusPwd());
            try {
                String[] masterUrl = getDbRealAddress(dataSource.getMasterUrl(), dataSource.getDsType()).split("&");
                String[] slaveUrl = getDbRealAddress(dataSource.getSlaveUrl(), dataSource.getDsType()).split("&");
                dataSourceBean.setMasterUrlOri(masterUrl[0]);
                dataSourceBean.setMasterUrlIp(masterUrl[1]);
                dataSourceBean.setSlaveUrlOri(slaveUrl[0]);
                dataSourceBean.setSlaveUrlIp(slaveUrl[1]);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            List<DataSchema> dataSchemas = dataSchemaMapper.searchSchema(null, dataSource.getId(), null);
            List<String> schemas = new ArrayList<>();
            if (dataSchemas != null && !dataSchemas.isEmpty()) {
                schemas = dataSchemas.stream().map(DataSchema::getSchemaName).collect(Collectors.toList());
            }
            String s = schemas.toString();
            dataSourceBean.setSchemas(s.substring(1, s.length() - 1));
            result.add(dataSourceBean);
        });
        return result;
    }

    private String getDbRealAddress(String url, String dsType) throws Exception {
        Matcher matcher;
        Matcher matcherServiceName = null;
        String host;
        String port;
        String resultOri = "";
        String resultIp = "";
        String serviceName = null;
        if (dsType.equalsIgnoreCase("oracle")) {
            matcher = ORACLE_URL_PATTERN.matcher(url);
            matcherServiceName = ORACLE_SERVICE_PATTERN.matcher(url);
            if (matcherServiceName.find()) {
                serviceName = matcherServiceName.group(1);
            }
        } else {
            matcher = MYSQL_URL_PATTERN.matcher(StringUtils.lowerCase(url));
        }
        while (matcher.find()) {
            host = matcher.group(1);
            port = matcher.group(2);
            if (StringUtils.isNotBlank(resultOri)) {
                resultOri = resultOri + "," + host + ":" + port;
                resultIp = resultIp + "," + getIpByHostName(host) + ":" + port;
            } else {
                resultOri = host + ":" + port;
                resultIp = getIpByHostName(host) + ":" + port;
            }
            if (serviceName != null) {
                resultOri = resultOri + ":" + matcherServiceName.group(1);
                resultIp = resultIp + ":" + matcherServiceName.group(1);
            }
        }
        return resultOri + "&" + resultIp;
    }

    public Long getTableRows(Long schemaId, String tableName) throws Exception {
        DataTableBean dataTableBean = dataTableMapper.getTableByParams(schemaId.intValue(), tableName);
        return tableService.getTableRowsNum(dataTableBean.getId());
    }
}
