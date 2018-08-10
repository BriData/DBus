/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2017 Bridata
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

package com.creditease.dbus.ws.web;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;
import com.creditease.dbus.commons.*;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.log.processor.adapter.LogFilebeatAdapter;
import com.creditease.dbus.commons.log.processor.adapter.LogFlumeAdapter;
import com.creditease.dbus.commons.log.processor.adapter.LogUmsAdapter;
import com.creditease.dbus.commons.log.processor.parse.RuleGrammar;
import com.creditease.dbus.ws.domain.RuleInfo;
import com.creditease.dbus.commons.log.processor.rule.impl.Rules;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.enums.MessageEncodeType;
import com.creditease.dbus.mgr.base.ConfUtils;
import com.creditease.dbus.utils.ControlMessageSender;
import com.creditease.dbus.ws.common.*;
import com.creditease.dbus.ws.domain.*;
import com.creditease.dbus.ws.mapper.*;
import com.creditease.dbus.ws.service.*;
import com.creditease.dbus.ws.service.mybatis.MybatisTemplate;
import com.creditease.dbus.ws.service.table.MongoTableFetcher;
import com.creditease.dbus.ws.service.table.TableFetcher;
import com.creditease.dbus.ws.tools.ControlMessageSenderProvider;
import com.creditease.dbus.ws.tools.PlainLogKafkaConsumerProvider;
import com.creditease.dbus.ws.tools.ZookeeperServiceProvider;
import com.github.pagehelper.PageInfo;
import com.google.common.base.Joiner;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.sql.*;
import java.util.*;
import java.util.Date;

/**
 * 数据源resource,提供数据源的相关操作
 */
@Path("/tables")
@Consumes("application/json")
@Produces("application/json;charset=utf-8")
public class DataTableResource {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private static final String INITIAL_LOAD_DATA = "load-data";

    private TablesService service = TablesService.getService();


    /**
     * 根据id获取相应的table
     *
     * @param id table的ID
     * @return id所代表的table对象
     */

    @GET
    @Path("/{id:\\d+}")
    public Response findById(@PathParam("id") long id) {
        System.out.println("进入table");
        DataTable dataTable = service.getTableById(id);
        return Response.status(200).entity(dataTable).build();
    }

    /**
     * 根据状态获取相应的table
     *
     * @param status table的ID
     * @return id所代表的table对象
     */

    @GET
    @Path("/{status}")
    public Response findByStatus(@PathParam("status") String status) {
        List<DataTable> list = service.getTableByStatus(status);
        return Response.status(200).entity(list).build();
    }

    /**
     * 根据数据源ID获取table列表
     * @param dsID 如果数据源ID不存在则返回全部table
     * @return table列表
     */
        /*
        @GET
        @Path("/dsid/{dsid:[1-9]+}")
        public Response findTablesByDsID(@PathParam("dsid") long dsID) {
            List<DataTable> list = service.findTablesByDsID(dsID);
            return Response.status(200).entity(list).build();
        }
        */
    /**
     * 根据schemaID获取table列表
     * @param schemaID 如果schemaID不存在则返回全部table
     * @return table列表
     */
       /*
       @GET
       @Path("/schemaid/{schemaid:[1-9]+}")
       public Response findTablesBySchemaID(@PathParam("schemaid") long schemaID) {
          List<DataTable> list = service.findTablesBySchemaID(schemaID);
          return Response.status(200).entity(list).build();
       }
       */
    /**
     * 根据table名称模糊获取table列表
     * @return table列表
     */

    @GET
    @Path("/allTables")
    public Response findAllTables() {
        List<DataTable> list = service.findAllTables();
        return Response.status(200).entity(list).build();
    }

    /**
     * 根据数据源ID,schemaID以及table名称获取相关table列表
     *给rider3的接口
     * @param
     * @return table列表
     */

    @GET
    @Path("/riderSearch")
    public Response findTables(@QueryParam("dsId") Long dsID, @QueryParam("schemaId") String schemaID, @QueryParam("tableName") String tableName) {
        //List<DataTable> list = service.findTables(dsID, schemaID, tableName);
        List<DataTable> list = service.search(null);
        List<RiderTable> listRider =new ArrayList<RiderTable>();
        for(DataTable table:list ){
            boolean flag = false;
            if(table.getTableName().equals(table.getPhysicalTableRegex())) {
                flag = true;
            }
            String dsType = table.getDsType();
            String namespace;
            if(flag) {
                namespace = dsType + "." + table.getDsName() + "." + table.getSchemaName() + "." + table.getTableName() +
                        "." + table.getVersion() + "." + "0" + "." + "0";
            }
            else {
                namespace = dsType + "." + table.getDsName() + "." + table.getSchemaName() + "." + table.getTableName() +
                        "." + table.getVersion() + "." + "0" + "." + table.getPhysicalTableRegex();
            }
            RiderTable rTable = new RiderTable();
            rTable.setNamespace(namespace);
            rTable.setTopic(table.getOutputTopic());
            rTable.setId(table.getId());
            rTable.setCreateTime(table.getCreateTime());
            try {
                ZookeeperServiceProvider provider = ZookeeperServiceProvider.getInstance();
                Properties globalConf = provider.getZkService().getProperties(com.creditease.dbus.ws.common.Constants.GLOBAL_CONF);
                rTable.setKafka(globalConf.getProperty("bootstrap.servers"));
            }catch (Exception e) {
                return Response.status(200).entity(new Result(-1, e.getMessage())).build();
            }
            listRider.add(rTable);
        }
        return Response.status(200).entity(listRider).build();
    }


    @GET
    @Path("/confirmStatusChange")
    public Response confirmStatusChange(Map<String, Object> map) {
        int result;
        try {
            MybatisTemplate template = MybatisTemplate.template();
            result = template.update((session, args) -> {
                TableMapper tableMapper = session.getMapper(TableMapper.class);
                return tableMapper.confirmVerChange(Long.parseLong(map.get("tableId").toString()));
            });
            if(result != 1) {
                return Response.status(204).entity(new Result(-1, "未能发送确认消息")).build();
            }
            return Response.ok().entity(result).build();
        } catch (Exception e) {
            logger.error("Error encountered while confirm status change with parameter:{}", JSON.toJSONString(map), e);
            return Response.status(204).entity(new Result(-1, e.getMessage())).build();
        }
    }

    @GET
    @Path("/getVersionListByTableId")
    public Response getVersionListByTableId(Map<String, Object> map) {
        try {
            MybatisTemplate template = MybatisTemplate.template();
            List<TableVersion> result = template.query((session, args) -> {
                TableVersionMapper mapper = session.getMapper(TableVersionMapper.class);
                return mapper.getVersionListByTableId(Long.parseLong(map.get("tableId").toString()));
            });
            return Response.ok().entity(result).build();
        } catch (Exception e) {
            logger.error("Error encountered while search version information with parameter:{}", JSON.toJSONString(map), e);
            return Response.status(204).entity(new Result(-1, e.getMessage())).build();
        }
    }

    @GET
    @Path("/getVersionDetail")
    public Response getVersionDetail(Map<String, Object> map) {
        try {
            MybatisTemplate template = MybatisTemplate.template();
            List<TableVersionColumn> result1 = template.query((session, args) -> {
                TableVersionColumnMapper mapper = session.getMapper(TableVersionColumnMapper.class);
                return mapper.getVersionColumnDetail(Long.parseLong(map.get("versionId1").toString()));
            });
            List<TableVersionColumn> result2 = template.query((session, args) -> {
                TableVersionColumnMapper mapper = session.getMapper(TableVersionColumnMapper.class);
                return mapper.getVersionColumnDetail(Long.parseLong(map.get("versionId2").toString()));
            });
            Map<String, List<TableVersionColumn>> result = new HashMap<>();
            result.put("v1data", result1);
            result.put("v2data", result2);
            return Response.ok().entity(result).build();
        } catch (Exception e) {
            logger.error("Error encountered while search version column information with parameter:{}", JSON.toJSONString(map), e);
            return Response.status(204).entity(new Result(-1, e.getMessage())).build();
        }
    }

    @GET
    @Path("/desensitization")
    public Response desensitization(Map<String, Object> map) {
        try {
            MybatisTemplate template = MybatisTemplate.template();
            List<DesensitizationInformation> result = template.query((session, args) -> {
                DesensitizationMapper mapper = session.getMapper(DesensitizationMapper.class);
                List<DesensitizationInformation> informations = mapper.getDesensitizationInformation(map.get("tableId"));
                return informations;
            });
            return Response.ok().entity(result).build();
        } catch (Exception e) {
            logger.error("Error encountered while search desensitization information with parameter:{}", JSON.toJSONString(map), e);
            return Response.status(204).entity(new Result(-1, e.getMessage())).build();
        }
    }

    @GET
    @Path("/fetchTableColumns")
    public Response fetchTableColumns(Map<String, Object> map) {
        DataTable tableInformation = getDbusDataSource(map);
        if (tableInformation == null) return Response.status(204).entity(new Result(-1, "找不到数据源")).build();

        DbusDataSource source = new DbusDataSource();
        source.setDsType(tableInformation.getDsType());
        source.setMasterURL(tableInformation.getMasterUrl());
        source.setDbusUser(tableInformation.getDbusUser());
        source.setDbusPassword(tableInformation.getDbusPassword());

        try {
            TableFetcher fetcher = TableFetcher.getFetcher(source);
            List<Map<String, Object>> columnsInfomation = fetcher.fetchTableColumnsInformation(tableInformation);
            if (columnsInfomation.isEmpty()) {
                logger.error("无法获取表结构信息");
                return Response.status(204).entity(new Result(-1, "无法获取表结构信息")).build();
            }
            return Response.ok().entity(columnsInfomation).build();
        } catch (Exception e) {
            logger.error("Error encountered while search data table with parameter:{}", JSON.toJSONString(map), e);
            return Response.status(204).entity(new Result(-1, e.getMessage())).build();
        }
    }

    @GET
    @Path("/fetchEncodePlugin")
    public Response fetchEncodePlugin() {
        try {
            MybatisTemplate template = MybatisTemplate.template();
            List<EncodePlugin> result = template.query((session, args) -> {
                DesensitizationMapper mapper = session.getMapper(DesensitizationMapper.class);
                List<EncodePlugin> list = mapper.getEncodePlugin();
                return list;
            });
            return Response.ok().entity(result).build();
        } catch (Exception e) {
            logger.error("Error encountered while search encode plugin", e);
            return Response.status(204).entity(new Result(-1, e.getMessage())).build();
        }
    }

    @GET
    @Path("/fetchEncodeAlgorithms")
    public Response fetchEncodeAlgorithms() {
        List<Object> result = new ArrayList<>();
        for(MessageEncodeType messageEncodeType: MessageEncodeType.values()){
            result.add(messageEncodeType.toString().toLowerCase());
        }
        return Response.ok().entity(result).build();
    }

    @GET
    @Path("/changeDesensitization")
    public Response changeDesensitization(Map<String, Map<String, Object>> param) {
        System.out.println(param);
        for (String key : param.keySet()) {
            Map<String, Object> map = param.get(key);
            try {
                MybatisTemplate template = MybatisTemplate.template();
                DesensitizationInformation di = new DesensitizationInformation();
                if(map.get("sql_type").equals("update")) {
                    di.setId(Integer.valueOf(map.get("id").toString()));
                    if (map.get("plugin_id") != null && map.get("plugin_id") != "")
                        di.setPluginId(Integer.valueOf(map.get("plugin_id").toString()));
                    di.setEncodeType(map.get("encode_type").toString());
                    di.setEncodeParam(map.get("encode_param").toString());
                    di.setTruncate(Long.valueOf(map.get("truncate").toString()));
                    di.setUpdateTime(new Date(Long.valueOf(map.get("update_time").toString())));
                    int result = template.query((session, args) -> {
                        DesensitizationMapper mapper = session.getMapper(DesensitizationMapper.class);
                        return mapper.updateDesensitizationInformation(di);
                    });
                }
                else if(map.get("sql_type").equals("delete")) {
                    di.setId(Integer.valueOf(map.get("id").toString()));
                    int result = template.query((session, args) -> {
                        DesensitizationMapper mapper = session.getMapper(DesensitizationMapper.class);
                        return mapper.deleteDesensitizationInformation(di);
                    });
                }
                else if(map.get("sql_type").equals("insert")) {
                    di.setTableId(Long.valueOf(map.get("table_id").toString()));
                    di.setFieldName(map.get("field_name").toString());
                    if (map.get("plugin_id") != null && map.get("plugin_id") != "")
                        di.setPluginId(Integer.valueOf(map.get("plugin_id").toString()));
                    di.setEncodeType(map.get("encode_type").toString());
                    di.setEncodeParam(map.get("encode_param").toString());
                    di.setTruncate(Long.valueOf(map.get("truncate").toString()));
                    di.setUpdateTime(new Date(Long.valueOf(map.get("update_time").toString())));
                    int result = template.query((session, args) -> {
                        DesensitizationMapper mapper = session.getMapper(DesensitizationMapper.class);
                        return mapper.insertDesensitizationInformation(di);
                    });
                }

            } catch (Exception e) {
                logger.error("Error encountered while changing desensitization information with parameter:{}", JSON.toJSONString(map), e);
                return Response.status(204).entity(new Result(-1, e.getMessage())).build();
            }
        }
        return Response.ok().entity("成功修改脱敏信息").build();
    }


    private DataTable getDbusDataSource(Map<String, Object> map) {
        try {
            MybatisTemplate template = MybatisTemplate.template();
            DataTable source = template.query((session, args) -> {
                TableMapper mapper = session.getMapper(TableMapper.class);
                DataTable returnSources = mapper.findById(Long.valueOf(map.get("tableId").toString()).longValue());
                return returnSources;
            });
            if (source == null) {
                logger.error("找不到表{}所对应的数据源", map.get("tableId"));
                return null;
            }
            return source;
        } catch (Exception e) {
            logger.error("Error encountered while search desensitization information with parameter:{}", JSON.toJSONString(map), e);
            return null;
        }
    }

    /**
     * 根据数据源名称,schema名称以及table名称获取相关table列表
     * @return table列表
     */

    @POST
    @Path("/search")
    public Response searchDataTable(Map<String, Object> map) {
        try {
            PageInfo<DataTable> result = service.search(getInt(map, "pageNum"), getInt(map, "pageSize"), map);
            TableVersionService versionService = TableVersionService.getService();
            for(DataTable dataTable : result.getList()) {
                String verChangeHistory = dataTable.getVerChangeHistory();
                if(StringUtils.isEmpty(verChangeHistory)) continue;
                String versionsChangeHistory = StringUtils.EMPTY;
                for(String verId : verChangeHistory.split(",")) {
                    try {
                        TableVersion tableVersion = versionService.findById(Long.parseLong(verId));
                        if(tableVersion != null && tableVersion.getVersion()!=null) {
                            versionsChangeHistory += ","+tableVersion.getVersion();
                        }
                    } catch (NumberFormatException e) {
                        logger.warn("transform verId to version failed for verid: {}, error message: {}",verId, e);
                        versionsChangeHistory += ","+verId;
                    }
                }
                if(StringUtils.isNotEmpty(versionsChangeHistory)) {
                    versionsChangeHistory = versionsChangeHistory.substring(1);
                }
                dataTable.setVersionsChangeHistory(versionsChangeHistory);
            }
            return Response.ok().entity(result).build();
        } catch (Exception e) {
            logger.error("Error encountered while search data table with parameter:{}", JSON.toJSONString(map), e);
            return Response.status(200).entity(new Result(-1, e.getMessage())).build();
        }
    }

    /**
     * 该接口给EDP提供接口，不实现分页
     * @param map
     * @return
     */
    @GET
    @Path("/search")
    public Response search(Map<String, Object> map) {
        try {
            List<DataTable> result = service.search(map);
            return HttpHeaderUtils.allowCrossDomain(Response.ok()).entity(result).build();
        } catch (Exception e) {
            logger.error("Error encountered while search data table with parameter:{}", JSON.toJSONString(map), e);
            return Response.status(200).entity(new Result(-1, e.getMessage())).build();
        }
    }

    @GET
    @Path("/executeSql")
    public Response executeSql(Map<String, Object> map) {
        try {
            DbusDataSource ds = new DbusDataSource();
            ds.setDsType(map.get("dsType").toString());
            ds.setMasterURL(map.get("URL").toString());
            ds.setDbusUser(map.get("user").toString());
            ds.setDbusPassword(map.get("password").toString());
            TableFetcher fetcher = TableFetcher.getFetcher(ds);
            List<Map<String,Object>> list = fetcher.fetchTableColumn(map);
            return Response.ok().entity(list).build();
        } catch (Exception e) {
            logger.error("Error encountered while search data table with parameter:{}", JSON.toJSONString(map), e);
            return Response.status(200).entity(new Result(-1, e.getMessage())).build();
        }
    }


    /**
     * 获取versId为空的table列表操作
     *
     * @return table列表
     */
    /*
    @GET
    @Path("/init")
    public Response findTablesNoVer(@QueryParam("schemaId") Long schemaId) {
        List<DataTable> list = service.findTablesNoVer(schemaId);
        TableVersionService serviceVersion = TableVersionService.getService();
        TableMetaService serviceMeta = TableMetaService.getService();
        TableVersion version = new TableVersion();
        DataTable tempTable = new DataTable();

        DataSourceService dsService = DataSourceService.getService();
        DbusDataSource ds = null;
        for (DataTable table : list) {
            version.setTableId(table.getId());
            version.setDsId(table.getDsID());
            version.setDbName(table.getDsName());
            version.setSchemaName(table.getSchemaName());
            version.setTableName(table.getTableName());
            version.setVersion(0);
            version.setInnerVersion(0);
            version.setEventOffset(0L);
            version.setEventPos(0L);
            serviceVersion.insertVersion(version);

            tempTable.setVerID(version.getId());
            service.updateDataTables(table.getId(), tempTable);
            if (ds == null) {
                ds = dsService.getDataSourceById(table.getDsID());
            }
            try {
                MetaFetcher fetcher = MetaFetcher.getFetcher(ds);

                Map<String, Object> map = new HashMap<>();
                map.put("schemaName", table.getSchemaName());
                map.put("tableName", table.getTableName());
                List<TableMeta> listMeta = fetcher.fetchMeta(map);
                for (TableMeta meta : listMeta) {
                    meta.setVerId(version.getId());
                    serviceMeta.insertMetas(meta);
                }
            } catch (Exception e) {
                logger.error("Error encountered while create initialize tables:{}", schemaId, e);
                return Response.status(200).entity(new Result(-1, "Error encountered while create initialize tables")).build();
            }

        }
        return Response.ok(Result.OK).build();
    }
*/
    /**
     * 根据id更新table
     *
     * @param
     * @param dt 数据
     * @return 更新状态
     */

    @POST
    @Path("/update")
    public Response updateDataTables( DataTable dt) {
        try {
            service.updateDataTables(dt);
            return Response.ok().entity(Result.OK).build();
        } catch (Exception ex) {
            logger.error("Error encountered while update Table with parameter {id:{}, ds:{}}", dt, ex);
            return Response.status(200).entity(new Result(-1, ex.getMessage())).build();
        }
    }

    /**
     * 新建table
     *
     * @param dt 数据
     * @return 返回新建状态
     */

    @POST
    public Response insertDataTables(DataTable dt) {
        try {
            service.insertDataTables(dt);
            return Response.ok().entity(Result.OK).build();
        } catch (Exception ex) {
            logger.error("Error encountered while create Table with parameter:{}", dt, ex);
            return Response.status(200).entity(new Result(-1, ex.getMessage())).build();
        }
    }

    /**
     * 激活指定的table,使该table生效
     *
     * @param id table的ID
     */
    @POST
    @Path("activate/{id:\\d+}")
    public Response activateDataTable(@PathParam("id") long id, Map<String, String> map) {
        try {
            ActiveTableParam param = ActiveTableParam.build(map);
            logger.info("Receive activateDataTable request, parameter[id:{}, param:{}]", id, JSON.toJSONString(param));
            DataTable table = service.getTableById(id);
            if (table == null) {
                Result result = new Result(-1, "Table not found!");
                logger.warn(JSON.toJSONString(result));
                return HttpHeaderUtils.allowCrossDomain(Response.ok()).entity(result).build();
            }

            DataSourceService dsService = DataSourceService.getService();
            DbusDataSource ds = dsService.getDataSourceById(table.getDsID());

            if (INITIAL_LOAD_DATA.equalsIgnoreCase(param.getType())) {
                DbusDatasourceType dsType = DbusDatasourceType.parse(ds.getDsType());
                InitialLoadService ilService = InitialLoadService.getService();

                FullPullHistory fullPullHistory = new FullPullHistory();
                Date date = new Date();
                fullPullHistory.setId(date.getTime());
                fullPullHistory.setType("normal");
                fullPullHistory.setDsName(map.get("dsName"));
                fullPullHistory.setSchemaName(map.get("schemaName"));
                fullPullHistory.setTableName(map.get("tableName"));
                fullPullHistory.setState("init");
                fullPullHistory.setInitTime(date);
                fullPullHistory.setUpdateTime(date);
                FullPullService.getService().insert(fullPullHistory);


                if (DbusDatasourceType.ORACLE == dsType) {
                    logger.info("Activate oracle table");
                    // 处理oracle拉全量
                    ilService.oracleInitialLoadBySql(ds, table, date.getTime());
                } else if (DbusDatasourceType.MYSQL == dsType) {
                    logger.info("Activate mysql table");
                    // 处理mysql拉全量
                    ilService.mysqlInitialLoadBySql(ds, table, date.getTime());
                } else {
                    throw new IllegalArgumentException("Illegal datasource type:" + ds.getDsType());
                }
            } else {
                /*Result result = validateTableStatus(table);
                if (result.getStatus() != 0) {
                    logger.warn("InitialLoad process is running, message:{}", result.getMessage());
                    return HttpHeaderUtils.allowCrossDomain(Response.ok()).entity(result).build();
                }*/

                // 不用拉全量的情况下直接发送 APPENDER_TOPIC_RESUME message 激活 appender
                ControlMessageSender sender = ControlMessageSenderProvider.getInstance().getSender();
                ControlMessage message = new ControlMessage();
                message.setId(System.currentTimeMillis());
                message.setFrom(com.creditease.dbus.ws.common.Constants.CONTROL_MESSAGE_SENDER_NAME);
                message.setType("APPENDER_TOPIC_RESUME");
                DataSchema schema = DataSchemaService.getService().getDataSchemaById(table.getSchemaID());
                message.addPayload("topic", schema.getSrcTopic());
                message.addPayload("SCHEMA_NAME", table.getSchemaName());
                message.addPayload("TABLE_NAME", table.getTableName());
                message.addPayload("STATUS", DataTable.OK);
                message.addPayload("VERSION", param.getVersion());

                sender.send(ds.getCtrlTopic(), message);
                logger.info("Control message sent, message:{}", message.toJSONString());
                // 如果需要更新version则
                if (param.getVersion() > 0) {
                    TableVersionService vs = TableVersionService.getService();
                    TableVersion v = new TableVersion();
                    v.setVersion(param.getVersion());
                    vs.updateVersion(table.getVerID(), v);
                }
            }
            logger.info("Activate DataTable request process ok");
            return HttpHeaderUtils.allowCrossDomain(Response.ok()).entity(Result.OK).build();
        } catch (Exception ex) {
            logger.error("Error encountered while active Table with parameter:{}", id, ex);
            return HttpHeaderUtils.allowCrossDomain(Response.status(200)).entity(new Result(-1, ex.getMessage())).build();
        }
    }

    /**
     * 使table无效
     *
     * @param id table的ID
     */
    @POST
    @Path("deactivate/{id:\\d+}")
    public Response deactivateDataTable(@PathParam("id") long id) {
        try {
            logger.info("Receive deactivateDataTable request, parameter[id:{}, param:{}]", id);
            DataTable table = service.getTableById(id);
            if (table == null) {
                Result result = new Result(-1, "Table not found!");
                logger.warn(JSON.toJSONString(result));
                return HttpHeaderUtils.allowCrossDomain(Response.ok()).entity(result).build();
            }

            Result result = validateTableStatus(table);
            if (result.getStatus() != 0) {
                logger.warn("InitialLoad process is running, message:{}", result.getMessage());
                return HttpHeaderUtils.allowCrossDomain(Response.ok().entity(result)).build();
            }

            DataSourceService dsService = DataSourceService.getService();
            DbusDataSource ds = dsService.getDataSourceById(table.getDsID());

            // 不用拉全量的情况下直接发送 APPENDER_TOPIC_RESUME message 激活 appender
            ControlMessageSender sender = ControlMessageSenderProvider.getInstance().getSender();
            ControlMessage message = new ControlMessage();
            message.setId(System.currentTimeMillis());
            message.setFrom(com.creditease.dbus.ws.common.Constants.CONTROL_MESSAGE_SENDER_NAME);
            message.setType("APPENDER_TOPIC_RESUME");

            DataSchema schema = DataSchemaService.getService().getDataSchemaById(table.getSchemaID());
            message.addPayload("topic", schema.getSrcTopic());
            message.addPayload("SCHEMA_NAME", table.getSchemaName());
            message.addPayload("TABLE_NAME", table.getTableName());
            message.addPayload("STATUS", DataTable.ABORT);
            message.addPayload("VERSION", 0);

            sender.send(ds.getCtrlTopic(), message);
            logger.info("Control message sent, message:{}", message.toJSONString());

            return HttpHeaderUtils.allowCrossDomain(Response.ok().entity(Result.OK)).build();
        } catch (Exception ex) {
            logger.error("Error encountered while deactivate Table with parameter:{}", id, ex);
            return HttpHeaderUtils.allowCrossDomain(Response.status(200)).entity(new Result(-1, ex.getMessage())).build();
        }
    }

    @GET
    @Path("/deleteTable")
    public Response deleteTable(Map<String, Object> map) {
        int tableId = Integer.parseInt(map.get("tableId").toString());
        int result = service.deleteTable(tableId);
        return Response.ok().entity(result).build();
    }

    /*
        @GET
        @Path("meta")
        public Response fetchMeta(@QueryParam("tableId") long tableId) {
            try {
                DataTable table = service.getTableById(tableId);
                DataSourceService dsService = DataSourceService.getService();
                DbusDataSource ds = dsService.getDataSourceById(table.getDsID());
                MetaFetcher fetcher = MetaFetcher.getFetcher(ds);

                Map<String, Object> map = new HashMap<>();
                map.put("schemaName", table.getSchemaName());
                map.put("tableName", table.getTableName());
                List<TableMeta> list = fetcher.fetchMeta(map);
                return Response.ok().entity(list).build();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return Response.ok(Result.OK).build();
        }
    */

    @GET
    @Path("/schemaname")
    public Response findTablesByDsIdAndSchemaName(@QueryParam("dsID") long dsID, @QueryParam("schemaName") String schemaName) {
        List<DataTable> list = service.findTablesByDsIdAndSchemaName(dsID, schemaName);
        return Response.status(200).entity(list).build();
    }

    @GET
    @Path("/tablename")
    public Response fetchTable(@QueryParam("dsName") String dsName,@QueryParam("schemaName") String schemaName) {
        try {
            DataSourceService dsService = DataSourceService.getService();
            DbusDataSource ds = dsService.getDataSourceByName(dsName);
            List<DataTable> list;
            if(DbusDatasourceType.stringEqual(ds.getDsType(),DbusDatasourceType.MYSQL)
                    || DbusDatasourceType.stringEqual(ds.getDsType(),DbusDatasourceType.ORACLE))
            {
                TableFetcher fetcher = TableFetcher.getFetcher(ds);
                Map<String, Object> map = new HashMap<>();
                map.put("dsName", dsName);
                map.put("schemaName", schemaName);
                list = fetcher.fetchTable(map);
            } else if(DbusDatasourceType.stringEqual(ds.getDsType(),DbusDatasourceType.MONGO)) {
                MongoTableFetcher fetcher = new MongoTableFetcher(ds);
                list = fetcher.fetchTable(schemaName);
            } else {
                throw new IllegalArgumentException("Unsupported datasource type");
            }

            return Response.ok().entity(list).build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Response.ok(Result.OK).build();
    }

    @POST
    @Path("/tablefield")
    public Response fetchTableField(Map<String, Object> param) {
        List<Map<String, Object>> paramsList = (List<Map<String, Object>>) param.get("sourceTables");
        List<List<TableMeta>> ret = new ArrayList<>();
        for (Map<String, Object> params : paramsList) {
            try {
                String dsName = String.valueOf(params.get("dsName"));
                DataSourceService dsService = DataSourceService.getService();
                DbusDataSource ds = dsService.getDataSourceByName(dsName);
                if(DbusDatasourceType.stringEqual(ds.getDsType(), DbusDatasourceType.MYSQL)
                        || DbusDatasourceType.stringEqual(ds.getDsType(), DbusDatasourceType.ORACLE)) {
                    TableFetcher fetcher = TableFetcher.getFetcher(ds);
                    ret = fetcher.fetchTableField(params, paramsList);

                } else if(DbusDatasourceType.stringEqual(ds.getDsType(), DbusDatasourceType.MONGO)){
                    MongoTableFetcher fetcher = new MongoTableFetcher(ds);
                    ret = fetcher.fetchTableField(params, paramsList);
                } else {
                    throw new IllegalArgumentException("Unsupported datasource type");
                }
                break;

            } catch (Exception e) {
                logger.error("Fetch table field failed, error message: {}", e);
            }
        }
        return Response.ok().entity(ret).build();
    }

    @GET
    @Path("/listPlainlogTable")
    public Response listPlainlogTable(Map<String, Object> map) {
        try {
            MybatisTemplate template = MybatisTemplate.template();
            List<DataTable> list = template.query((session, args) -> {
                TableMapper mapper = session.getMapper(TableMapper.class);
                return mapper.findBySchemaID(Long.parseLong(map.get("schemaId").toString()));
            });
            return Response.ok(list).build();
        } catch (Exception e) {
            logger.error("list plain log tables error", e);
            return Response.status(204).entity(new Result(-1, e.getMessage())).build();
        }
    }

    @GET
    @Path("/createPlainlogTable")
    public Response createPlainlogTable(Map<String, Object> map) {
        try {
            DataTable dataTable = JSON.parseObject(JSON.toJSONString(map), DataTable.class);
            MybatisTemplate template = MybatisTemplate.template();
            int res = template.query((session, args) -> {
                TableMapper mapper = session.getMapper(TableMapper.class);
                return mapper.insert(dataTable);
            });
            if (res != 1) throw new Exception("insert plain log table error");

            TableVersion tableVersion = new TableVersion();
            tableVersion.setTableId(dataTable.getId());
            tableVersion.setDsId(dataTable.getDsID());
            tableVersion.setDbName(dataTable.getDsName());
            tableVersion.setSchemaName(dataTable.getSchemaName());
            tableVersion.setTableName(dataTable.getTableName());
            tableVersion.setVersion(0);
            tableVersion.setInnerVersion(0);
            tableVersion.setEventOffset(0L);
            tableVersion.setEventPos(0L);
            res = template.query((session, args) -> {
                TableVersionMapper mapper = session.getMapper(TableVersionMapper.class);
                return mapper.insert(tableVersion);
            });

            if (res != 1) throw new Exception("insert plain log tableVersion error");

            dataTable.setVerID(tableVersion.getId());
            res = template.query((session, args) -> {
                TableMapper mapper = session.getMapper(TableMapper.class);
                return mapper.update(dataTable);
            });

            if (res != 1) throw new Exception("update plain log table verId error");

            return listPlainlogTable(map);
        } catch (Exception e) {
            logger.error("create plain log tables error", e);
            return Response.status(204).entity(new Result(-1, e.getMessage())).build();
        }
    }

    @GET
    @Path("/createPlainlogSchemaAndTable")
    public Response createPlainlogSchemaAndTable(Map<String, Object> map) {
        try {
            DataSchema dataSchema = JSON.parseObject(JSON.toJSONString(map.get("schema")), DataSchema.class);
            MybatisTemplate template = MybatisTemplate.template();
            int res = template.query((session, args) -> {
                DataSchemaMapper mapper = session.getMapper(DataSchemaMapper.class);
                return mapper.insert(dataSchema);
            });
            if (res != 1) throw new Exception("insert plain log schema error");
            List<JSONObject> tables = JSON.parseArray(JSON.toJSONString(map.get("tables")), JSONObject.class);

            for (JSONObject table : tables) {
                DataTable dataTable = new DataTable();
                dataTable.setDsID(dataSchema.getDsId());
                dataTable.setSchemaID(dataSchema.getId());
                dataTable.setSchemaName(dataSchema.getSchemaName());
                dataTable.setTableName(table.getString("tableName"));
                dataTable.setPhysicalTableRegex(table.getString("tableName"));
                dataTable.setOutputTopic(table.getString("outputTopic"));
                dataTable.setStatus(DataTable.ABORT);
                res = template.query((session, args) -> {
                    TableMapper mapper = session.getMapper(TableMapper.class);
                    return mapper.insert(dataTable);
                });
                if (res != 1) throw new Exception("insert plain log table error");

                TableVersion tableVersion = new TableVersion();
                tableVersion.setTableId(dataTable.getId());
                tableVersion.setDsId(dataSchema.getDsId());
                tableVersion.setDbName(dataSchema.getDsName());
                tableVersion.setSchemaName(dataSchema.getSchemaName());
                tableVersion.setTableName(dataTable.getTableName());
                tableVersion.setVersion(0);
                tableVersion.setInnerVersion(0);
                tableVersion.setEventOffset(0L);
                tableVersion.setEventPos(0L);
                res = template.query((session, args) -> {
                    TableVersionMapper mapper = session.getMapper(TableVersionMapper.class);
                    return mapper.insert(tableVersion);
                });

                if (res != 1) throw new Exception("insert plain log tableVersion error");

                dataTable.setVerID(tableVersion.getId());
                res = template.query((session, args) -> {
                    TableMapper mapper = session.getMapper(TableMapper.class);
                    return mapper.update(dataTable);
                });

                if (res != 1) throw new Exception("update plain log table verId error");
            }
            return Response.ok().build();
        } catch (Exception e) {
            logger.error("create plain log schema and tables error", e);
            return Response.status(204).entity(new Result(-1, e.getMessage())).build();
        }
    }
    // 规则组开始

    @GET
    @Path("/getAllRuleGroup")
    public Response getAllRuleGroup(Map<String, Object> map) {
        Connection connection;
        try {
            MybatisTemplate template = MybatisTemplate.template();
            List<DataTableRuleGroup> list = template.query((session, args) -> {
                DataTableRuleMapper mapper = session.getMapper(DataTableRuleMapper.class);
                return mapper.getAllRuleGroup(Long.parseLong(map.get("tableId").toString()));
            });

            Class.forName("com.mysql.jdbc.Driver");
            Properties properties = ConfUtils.mybatisConf;
            connection = DriverManager.getConnection(properties.getProperty("url"), properties.getProperty("username"), properties.getProperty("password"));
            List<RuleInfo> ruleInfos = getAsRuleInfo(connection, map);

            Map<String, Object> ret = new HashMap<>();
            ret.put("group", list);
            ret.put("saveAs", ruleInfos);

            return Response.ok().entity(ret).build();
        } catch (Exception e) {
            logger.error("Error encountered while get all rule group with parameter:{}", JSON.toJSONString(map), e);
            return Response.status(204).entity(new Result(-1, e.getMessage())).build();
        }
    }

    @GET
    @Path("/updateRuleGroup")
    public Response updateRuleGroup(Map<String, Object> map) {

        DataTableRuleGroup group = new DataTableRuleGroup();
        group.setId(Long.parseLong(map.get("groupId").toString()));
        group.setStatus(map.get("newStatus").toString());
        group.setGroupName(map.get("newName").toString());

        try {
            MybatisTemplate template = MybatisTemplate.template();
            int updateResult = template.query((session, args) -> {
                DataTableRuleMapper mapper = session.getMapper(DataTableRuleMapper.class);
                return mapper.updateRuleGroup(group);
            });
            if (updateResult == 0) {
                throw new SQLException();
            }

            return getAllRuleGroup(map);

        } catch (Exception e) {
            logger.error("update group info failed with parameter:{}", JSON.toJSONString(map), e);
            return Response.status(204).entity(new Result(-1, e.getMessage())).build();
        }
    }

    @GET
    @Path("/deleteRuleGroup")
    public Response deleteRuleGroup(Map<String, Object> map) {
        try {
            MybatisTemplate template = MybatisTemplate.template();
            int deleteResult = template.query((session, args) -> {
                DataTableRuleMapper mapper = session.getMapper(DataTableRuleMapper.class);
                int r1 = mapper.deleteRuleGroup(Long.parseLong(map.get("groupId").toString()));
                int r2 = mapper.deleteRules(Long.parseLong(map.get("groupId").toString()));
                return r1 + r2;
            });
            if (deleteResult == 0) {
                throw new SQLException();
            }

            return getAllRuleGroup(map);

        } catch (Exception e) {
            logger.error("update group info failed with parameter:{}", JSON.toJSONString(map), e);
            return Response.status(204).entity(new Result(-1, e.getMessage())).build();
        }
    }

    @GET
    @Path("/addGroup")
    public Response addGroup(Map<String, Object> map) {
        try {
            MybatisTemplate template = MybatisTemplate.template();
            int insertResult = template.query((session, args) -> {
                DataTableRuleMapper mapper = session.getMapper(DataTableRuleMapper.class);
                DataTableRuleGroup group = new DataTableRuleGroup();
                group.setTableId(Long.parseLong(map.get("tableId").toString()));
                group.setStatus(map.get("newStatus").toString());
                group.setGroupName(map.get("newName").toString());
                return mapper.addGroup(group);
            });
            if (insertResult == 0) {
                throw new SQLException();
            }

            return getAllRuleGroup(map);

        } catch (Exception e) {
            logger.error("update group info failed with parameter:{}", JSON.toJSONString(map), e);
            return Response.status(204).entity(new Result(-1, e.getMessage())).build();
        }
    }

    @GET
    @Path("/cloneRuleGroup")
    public Response cloneRuleGroup(Map<String, Object> map) {

        // 生成新的规则组信息
        DataTableRuleGroup group = new DataTableRuleGroup();
        group.setTableId(Long.parseLong(map.get("tableId").toString()));
        group.setGroupName(map.get("newName").toString());
        group.setStatus(map.get("newStatus").toString());

        try {

            //插入新的规则组，获取其生成的id
            MybatisTemplate template = MybatisTemplate.template();
            int insertResult = template.query((session, args) -> {
                DataTableRuleMapper mapper = session.getMapper(DataTableRuleMapper.class);
                return mapper.addGroup(group);
            });
            if (insertResult == 0) {
                throw new SQLException();
            }

            //获取原来这个组的所有规则
            List<DataTableRule> rules = template.query((session, args) -> {
                DataTableRuleMapper mapper = session.getMapper(DataTableRuleMapper.class);
                return mapper.getAllRules(Long.parseLong(map.get("groupId").toString()));
            });

            if(rules != null && rules.size() > 0) {
                //修改规则的groupId
                for (DataTableRule rule : rules) {
                    rule.setGroupId(group.getId());
                }

                //插入这些规则
                insertResult = template.query((session, args) -> {
                    DataTableRuleMapper mapper = session.getMapper(DataTableRuleMapper.class);
                    return mapper.saveAllRules(rules);
                });
                if (insertResult != rules.size()) {
                    throw new SQLException();
                }
            }

            return getAllRuleGroup(map);

        } catch (Exception e) {
            logger.error("clone group and rules failed with parameter:{}", JSON.toJSONString(map), e);
            return Response.status(204).entity(new Result(-1, e.getMessage())).build();
        }
    }

    @GET
    @Path("/diffGroupRule")
    public Response diffGroupRule(Map<String, Object> map) throws SQLException {
        Connection connection = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Properties properties = ConfUtils.mybatisConf;
            connection = DriverManager.getConnection(properties.getProperty("url"), properties.getProperty("username"), properties.getProperty("password"));

            List<RuleInfo> result = getAsRuleInfo(connection, map);

            return Response.ok().entity(result).build();
        } catch (Exception e) {
            logger.error("Get group rule diff info failed with parameter:{}", JSON.toJSONString(map), e);
            return Response.status(204).entity(new Result(-1, e.getMessage())).build();
        } finally {
            if(connection != null) {
                connection.close();
            }
        }
    }

    @GET
    @Path("/upgradeVersion")
    public Response upgradeVersion(Map<String, Object> map) throws SQLException {
        Connection connection = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Properties properties = ConfUtils.mybatisConf;
            connection = DriverManager.getConnection(properties.getProperty("url"), properties.getProperty("username"), properties.getProperty("password"));
            connection.setAutoCommit(false);


            String validateResult = validateAs(connection, map);
            if(StringUtils.isNotEmpty(validateResult)) {
                return Response.accepted().entity(new Result(-1, validateResult)).build();
            }
            /**
             * 在t_meta_version中生成新版本号，同时更新t_data_table中的版本
             */
            int verId = generateNewVerId(connection, map);
            /**
             * 从t_plain_log_rule_group和t_plain_log_rules中复制规则到
             * t_plain_log_rule_group_version和t_plain_log_rules_version中
             */
            confirmUpgradeVersion(connection, map, verId);
            /**
             * 向t_table_meta中插入 ruleScope,name,type
             */
            upgradeRuleTableMeta(connection, map, verId);

            connection.commit();

            return Response.ok().build();
        } catch (Exception e) {
            logger.error("upgrade rule version failed with parameter:{}", JSON.toJSONString(map), e);
            return Response.status(204).entity(new Result(-1, e.getMessage())).build();
        } finally {
            if(connection != null) {
                connection.close();
            }
        }
    }

    private List<RuleInfo> getAsRuleInfo(Connection connection, Map<String, Object> map) throws SQLException {
        String sql = "select tg.id, tg.group_name, tg.`status`, tr.rule_grammar from (select id,group_name,status from t_plain_log_rule_group where table_id= ? ) tg left join (select group_id,rule_grammar from t_plain_log_rules where rule_type_name = ? ) tr on tg.id = tr.group_id";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setInt(1, Integer.parseInt(map.get("tableId").toString()));
        ps.setString(2, Rules.SAVEAS.name);
        ResultSet rs = ps.executeQuery();
        List<RuleInfo> ruleInfos = new ArrayList<>();
        while(rs.next()) {
            RuleInfo ruleInfo = new RuleInfo();
            ruleInfo.setGroupId(rs.getLong("id"));
            ruleInfo.setGroupName(rs.getString("group_name"));
            ruleInfo.setRuleGrammar(rs.getString("rule_grammar"));
            ruleInfo.setStatus(rs.getString("status"));
            ruleInfos.add(ruleInfo);
        }
        rs.close();
        ps.close();
        return ruleInfos;
    }
    private String validateAs(Connection connection, Map<String, Object> map) throws SQLException {
        final String INACTIVE = "inactive";
        List<RuleInfo> ruleInfos = getAsRuleInfo(connection, map);
        for (int i = 0; i < ruleInfos.size(); i++) {
            if (ruleInfos.get(i).getStatus().equals(INACTIVE)) continue;
            for (int j = i + 1; j < ruleInfos.size(); j++) {
                if (ruleInfos.get(j).getStatus().equals(INACTIVE)) continue;
                List<RuleGrammar> ruleGrammar1 = JSON.parseArray(ruleInfos.get(i).getRuleGrammar(), RuleGrammar.class);
                List<RuleGrammar> ruleGrammar2 = JSON.parseArray(ruleInfos.get(j).getRuleGrammar(), RuleGrammar.class);
                if (!compareRuleGrammarAsType(ruleGrammar1, ruleGrammar2)) {
                    return ruleInfos.get(i).getGroupName() + " 和 " + ruleInfos.get(j).getGroupName() + " SaveAS不相同，请使用Diff对比";
                }
            }
        }
        return null;
    }

    private boolean compareRuleGrammarAsType(List<RuleGrammar> ruleGrammar1, List<RuleGrammar> ruleGrammar2) {
        if(ruleGrammar1 == null || ruleGrammar2 == null) return false;
        if(ruleGrammar1.size() != ruleGrammar2.size()) return false;
        Collections.sort(ruleGrammar1, (o1, o2) -> StringUtils.compare(o1.getName(), o2.getName()));
        Collections.sort(ruleGrammar2, (o1, o2) -> StringUtils.compare(o1.getName(), o2.getName()));
        for(int i=0;i<ruleGrammar1.size();i++) {
            if (!StringUtils.equals(ruleGrammar1.get(i).getName(), ruleGrammar2.get(i).getName())) return false;
            if (!StringUtils.equals(ruleGrammar1.get(i).getType(), ruleGrammar2.get(i).getType())) return false;
            if (!StringUtils.equals(ruleGrammar1.get(i).getRuleScope(), ruleGrammar2.get(i).getRuleScope())) return false;
        }
        return true;
    }

    private void upgradeRuleTableMeta(Connection connection, Map<String, Object> map, int verId) throws SQLException {
        List<RuleInfo> ruleInfos = getAsRuleInfo(connection, map);
        RuleInfo asRuleInfo = null;
        for (RuleInfo ruleInfo : ruleInfos) {
            if ("active".equals(ruleInfo.getStatus())) asRuleInfo = ruleInfo;
        }
        if (asRuleInfo == null) return;
        List<RuleGrammar> ruleGrammars = JSON.parseArray(asRuleInfo.getRuleGrammar(), RuleGrammar.class);
        MybatisTemplate template = MybatisTemplate.template();
        for (RuleGrammar ruleGrammar : ruleGrammars) {
            TableMeta tableMeta = new TableMeta();
            tableMeta.setVerId((long) verId);
            tableMeta.setColumnName(ruleGrammar.getName());
            tableMeta.setColumnId(Integer.parseInt(ruleGrammar.getRuleScope()));
            tableMeta.setOriginalSer(0L);
            tableMeta.setDataType(ruleGrammar.getType());
            tableMeta.setDataLength(0);
            tableMeta.setDataPrecision(0);
            tableMeta.setDataScale(0);
            tableMeta.setNullable("Y");
            tableMeta.setIsPk("N");
            tableMeta.setPkPosition(-1);
            int result = template.query((session, args) -> {
                TableMetaMapper mapper = session.getMapper(TableMetaMapper.class);
                return mapper.insert(tableMeta);
            });
        }
    }

    private void confirmUpgradeVersion(Connection connection, Map<String, Object> map, int verId) throws SQLException {
        int tableId = Integer.parseInt(map.get("tableId").toString());
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        String sql = "SELECT `id`, `table_id`, `group_name`, `status` FROM `t_plain_log_rule_group` WHERE `table_id` = ? AND `status` = 'active';";
        PreparedStatement selectTempGroupPS = connection.prepareStatement(sql);
        selectTempGroupPS.setInt(1, tableId);
        ResultSet tempGroup = selectTempGroupPS.executeQuery();

        while(tempGroup.next()) {

            sql = "SELECT `order_id`, `rule_type_name`, `rule_grammar` FROM `t_plain_log_rules` where group_id = ?;";
            PreparedStatement selectTempRulesPS = connection.prepareStatement(sql);
            selectTempRulesPS.setInt(1, tempGroup.getInt("id"));
            ResultSet tempRules = selectTempRulesPS.executeQuery();

            sql = "INSERT INTO `t_plain_log_rule_group_version` (`table_id`, `group_name`, `status`, `ver_id`, `update_time`) VALUES (?,?,?,?,?);";
            PreparedStatement insertNewGroupVersionPS = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            insertNewGroupVersionPS.setInt(1, tableId);
            insertNewGroupVersionPS.setString(2, tempGroup.getString("group_name"));
            insertNewGroupVersionPS.setString(3, tempGroup.getString("status"));
            insertNewGroupVersionPS.setInt(4, verId);
            insertNewGroupVersionPS.setTimestamp(5, timestamp);
            insertNewGroupVersionPS.executeUpdate();
            ResultSet idSet = insertNewGroupVersionPS.getGeneratedKeys();
            idSet.next();
            int groupId = idSet.getInt("generated_key");
            idSet.close();
            insertNewGroupVersionPS.close();

            sql = "INSERT INTO `t_plain_log_rules_version` (`group_id`, `order_id`, `rule_type_name`, `rule_grammar`, `update_time`) VALUES (?,?,?,?,?);";
            PreparedStatement insertNewRulesPS = connection.prepareStatement(sql);
            while(tempRules.next()) {
                insertNewRulesPS.setInt(1, groupId);
                insertNewRulesPS.setInt(2, tempRules.getInt("order_id"));
                insertNewRulesPS.setString(3, tempRules.getString("rule_type_name"));
                insertNewRulesPS.setString(4, tempRules.getString("rule_grammar"));
                insertNewRulesPS.setTimestamp(5, timestamp);
                insertNewRulesPS.addBatch();
            }
            insertNewRulesPS.executeBatch();

            insertNewRulesPS.close();

            tempRules.close();
            selectTempRulesPS.close();
        }

        tempGroup.close();
        selectTempGroupPS.close();

    }

    private int generateNewVerId(Connection connection, Map<String, Object> map) throws SQLException {
        int tableId = Integer.parseInt(map.get("tableId").toString());
        int verId;

        String sql = "select `id`, `table_id`, `ds_id`, `db_name`, `schema_name`, `table_name`, `version`, `inner_version`, `event_offset`, `event_pos`, `update_time`, `comments` from t_meta_version where table_id = ? order by version desc limit 1;";
        PreparedStatement oldVersionPS = connection.prepareStatement(sql);
        oldVersionPS.setInt(1, tableId);
        ResultSet oldVersion = oldVersionPS.executeQuery();

        if (oldVersion.next()) {
            sql = "INSERT INTO `t_meta_version` (`table_id`, `ds_id`, `db_name`, `schema_name`, `table_name`, `version`, `inner_version`, `event_offset`, `event_pos`, `update_time`, `comments`) VALUES (?,?,?,?,?,?,?,?,?,?,?);";
            PreparedStatement copyNewVersionPS = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            copyNewVersionPS.setInt(1, tableId);
            copyNewVersionPS.setInt(2, oldVersion.getInt("ds_id"));
            copyNewVersionPS.setString(3, oldVersion.getString("db_name"));
            copyNewVersionPS.setString(4, oldVersion.getString("schema_name"));
            copyNewVersionPS.setString(5, oldVersion.getString("table_name"));
            copyNewVersionPS.setInt(6, oldVersion.getInt("version") + 1);
            copyNewVersionPS.setInt(7, oldVersion.getInt("inner_version") + 1);
            copyNewVersionPS.setLong(8, oldVersion.getLong("event_offset"));
            copyNewVersionPS.setLong(9, oldVersion.getLong("event_pos"));
            copyNewVersionPS.setTimestamp(10, new Timestamp(System.currentTimeMillis()));
            copyNewVersionPS.setString(11, oldVersion.getString("comments"));
            copyNewVersionPS.executeUpdate();
            ResultSet idSet = copyNewVersionPS.getGeneratedKeys();
            idSet.next();
            verId = idSet.getInt("generated_key");
            idSet.close();
            copyNewVersionPS.close();
        } else {
            int dsId = Integer.parseInt(map.get("dsId").toString());
            String dsName = map.get("dsName").toString();
            String schemaName = map.get("schemaName").toString();
            String tableName = map.get("tableName").toString();

            sql = "INSERT INTO `t_meta_version` (`table_id`, `ds_id`, `db_name`, `schema_name`, `table_name`, `version`, `inner_version`, `event_offset`, `event_pos`, `update_time`, `comments`) VALUES (?,?,?,?,?,?,?,?,?,?,?);";
            PreparedStatement newVersionPS = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            newVersionPS.setInt(1, tableId);
            newVersionPS.setInt(2, dsId);
            newVersionPS.setString(3, dsName);
            newVersionPS.setString(4, schemaName);
            newVersionPS.setString(5, tableName);
            newVersionPS.setInt(6, 0);
            newVersionPS.setInt(7, 0);
            newVersionPS.setLong(8, 0);
            newVersionPS.setLong(9, 0);
            newVersionPS.setTimestamp(10, new Timestamp(System.currentTimeMillis()));
            newVersionPS.setString(11, null);
            newVersionPS.executeUpdate();
            ResultSet idSet = newVersionPS.getGeneratedKeys();
            idSet.next();
            verId = idSet.getInt("generated_key");
            idSet.close();
            newVersionPS.close();
        }

        oldVersion.close();
        oldVersionPS.close();

        sql = "UPDATE `t_data_tables` SET `ver_id`= ? WHERE `id`= ?;";
        PreparedStatement updateDataTablePS = connection.prepareStatement(sql);
        updateDataTablePS.setInt(1, verId);
        updateDataTablePS.setInt(2, tableId);
        updateDataTablePS.executeUpdate();
        updateDataTablePS.close();

        return verId;
    }

    // 规则组结束

    // ResultSet转List<Map>
//    private List<Map<String, Object>> convertResultSetToList(ResultSet resultSet) throws SQLException {
//        List<Map<String, Object>> list = new ArrayList();
//        ResultSetMetaData md = resultSet.getMetaData(); //获得结果集结构信息,元数据
//        int columnCount = md.getColumnCount();   //获得列数
//        while (resultSet.next()) {
//            Map<String, Object> map = new HashMap<>();
//            for (int i = 1; i <= columnCount; i++) {
//                map.put(md.getColumnName(i), resultSet.getObject(i));
//            }
//            list.add(map);
//        }
//        return list;
//    }


    // 规则开始

    @GET
    @Path("/getAllRules")
    public Response getAllRules(Map<String, Object> map) {

        String dsType = map.get("dsType").toString();

        try {
            MybatisTemplate template = MybatisTemplate.template();
            List<DataTableRule> rules = template.query((session, args) -> {
                DataTableRuleMapper mapper = session.getMapper(DataTableRuleMapper.class);
                return mapper.getAllRules(Long.parseLong(map.get("groupId").toString()));
            });
            // 获取该表的数据源topic
            String topic;
            DbusDataSource dataSource = template.query((session, args) -> {
                DataSourceMapper mapper = session.getMapper(DataSourceMapper.class);
                return mapper.findById(Long.parseLong(map.get("dsId").toString()));
            });
            topic = dataSource.getTopic();

            // 获取该表的数据源offset

            KafkaConsumer<String, String> consumer = PlainLogKafkaConsumerProvider.getKafkaConsumer();
            TopicPartition dataTopicPartition = new TopicPartition(topic, 0);
            List<TopicPartition> topics = Arrays.asList(dataTopicPartition);
            consumer.assign(topics);
            consumer.seekToEnd(topics);
            long offset = consumer.position(dataTopicPartition);

            Map<String, Object> result = new HashMap<>();
            result.put("topic",topic);
            result.put("offset",offset);
            result.put("rules",rules);
            return Response.ok().entity(result).build();

        } catch (Exception e) {
            logger.error("Error encountered while get all rules with parameter:{}", JSON.toJSONString(map), e);
            return Response.status(204).entity(new Result(-1, e.getMessage())).build();
        }
    }

    @POST
    @Path("/saveAllRules")
    public Response saveAllRules(Map<String, Object> map) {
        try {
            MybatisTemplate template = MybatisTemplate.template();
            int result = template.query((session, args) -> {
                DataTableRuleMapper mapper = session.getMapper(DataTableRuleMapper.class);

                DataTableRuleGroup group = new DataTableRuleGroup();
                group.setId(Long.parseLong(map.get("groupId").toString()));

                return mapper.updateRuleGroup(group);
            });
            result = template.query((session, args) -> {
                DataTableRuleMapper mapper = session.getMapper(DataTableRuleMapper.class);
                return mapper.deleteRules(Long.parseLong(map.get("groupId").toString()));
            });
            result = template.query((session, args) -> {
                DataTableRuleMapper mapper = session.getMapper(DataTableRuleMapper.class);
                return mapper.saveAllRules(JSON.parseArray(map.get("rules").toString(), DataTableRule.class));
            });

            return Response.ok().build();

        } catch (Exception e) {
            logger.error("Error encountered while save all rules with parameter:{}", JSON.toJSONString(map), e);
            return Response.status(204).entity(new Result(-1, e.getMessage())).build();
        }
    }

    @POST
    @Path("/executeRules")
    public Response executeRules(Map<String, Object> map) {
        try {
            Map<String, Object> kafkaData = getKafkaPlainLogContent(map);
            List<List<String>> keysList = (List<List<String>>) kafkaData.get("keysList");
            List<List<String>> valuesList = (List<List<String>>) kafkaData.get("valuesList");
            List<RuleInfo> executeRules = JSON.parseArray(map.get("executeRules").toString(), RuleInfo.class);
            List<List<String>> data = new ArrayList<>();

            if(valuesList == null || executeRules == null) {
                return Response.ok().entity(valuesList).build();
            }

            DbusDatasourceType dsType = DbusDatasourceType.parse(map.get("dsType").toString().toLowerCase());

            List<Long> offset = new ArrayList<>();
            long kafkaOffset = Long.parseLong(map.get("kafkaOffset").toString());

            for (int i = 0; i < valuesList.size(); i++) {

                List<String> rowList = valuesList.get(i);

                if (dsType.equals(DbusDatasourceType.LOG_UMS)) {
                    if(executeRules.size() == 0) {
                        data.add(rowList);
                        offset.add(kafkaOffset + i);
                    } else {
                        LogUmsAdapter adapter = new LogUmsAdapter(rowList.get(0));
                        while (adapter.hasNext()) {
                            rowList = new ArrayList() {{
                                add(adapter.next());
                            }};
                            for (RuleInfo rule : executeRules) {
                                String ruleGramar = rule.getRuleGrammar();
                                List<RuleGrammar> grammar = JSON.parseArray(ruleGramar, RuleGrammar.class);
                                Rules rules = Rules.fromStr(rule.getRuleTypeName());
                                rowList = rules.getRule().transform(rowList, grammar, rules);
                                if (rowList.isEmpty()) break;
                            }
                            if (!rowList.isEmpty()) {
                                data.add(rowList);
                                offset.add(kafkaOffset + i);
                            }
                        }
                    }
                } else if (dsType.equals(DbusDatasourceType.LOG_LOGSTASH)
                        || dsType.equals(DbusDatasourceType.LOG_LOGSTASH_JSON)) {
                    for (RuleInfo rule : executeRules) {
                        String ruleGramar = rule.getRuleGrammar();
                        List<RuleGrammar> grammar = JSON.parseArray(ruleGramar, RuleGrammar.class);
                        Rules rules = Rules.fromStr(rule.getRuleTypeName());
                        rowList = rules.getRule().transform(rowList, grammar, rules);
                        if (rowList.isEmpty()) break;
                    }
                    if (!rowList.isEmpty()) {
                        data.add(rowList);
                        offset.add(kafkaOffset + i);
                    }
                } else if (dsType.equals(DbusDatasourceType.LOG_FLUME)) {
                    LogFlumeAdapter adapter = new LogFlumeAdapter(keysList.get(i).get(0), rowList.get(0));
                    while (adapter.hasNext()) {
                        rowList = new ArrayList() {{
                            add(adapter.next());
                        }};
                        for (RuleInfo rule : executeRules) {
                            String ruleGramar = rule.getRuleGrammar();
                            List<RuleGrammar> grammar = JSON.parseArray(ruleGramar, RuleGrammar.class);
                            Rules rules = Rules.fromStr(rule.getRuleTypeName());
                            rowList = rules.getRule().transform(rowList, grammar, rules);
                            if (rowList.isEmpty()) break;
                        }
                        if (!rowList.isEmpty()) {
                            data.add(rowList);
                            offset.add(kafkaOffset + i);
                        }
                    }
                } else if (dsType.equals(DbusDatasourceType.LOG_FILEBEAT)) {
                    LogFilebeatAdapter adapter = new LogFilebeatAdapter(rowList.get(0));
                    while (adapter.hasNext()) {
                        rowList = new ArrayList() {{
                            add(adapter.next());
                        }};
                        for (RuleInfo rule : executeRules) {
                            String ruleGramar = rule.getRuleGrammar();
                            List<RuleGrammar> grammar = JSON.parseArray(ruleGramar, RuleGrammar.class);
                            Rules rules = Rules.fromStr(rule.getRuleTypeName());
                            rowList = rules.getRule().transform(rowList, grammar, rules);
                            if (rowList.isEmpty()) break;
                        }
                        if (!rowList.isEmpty()) {
                            data.add(rowList);
                            offset.add(kafkaOffset + i);
                        }
                    }
                }
            }

            HashMap<String, Object> ret = new HashMap<>();
            ret.put("data", data);
            ret.put("offset", offset);
            if (executeRules.size() == 0) {
                if (dsType.equals(DbusDatasourceType.LOG_UMS)) {
                    ret.put("dataType", "UMS");
                } else if (dsType.equals(DbusDatasourceType.LOG_LOGSTASH)
                        || dsType.equals(DbusDatasourceType.LOG_LOGSTASH_JSON)
                        || dsType.equals(DbusDatasourceType.LOG_FLUME)
                        || dsType.equals(DbusDatasourceType.LOG_FILEBEAT)) {
                    ret.put("dataType", "JSON");
                }
            } else {
                String lastRule = executeRules.get(executeRules.size() - 1).getRuleTypeName();
                if (lastRule.equals(Rules.FLATTENUMS.name)
                        || lastRule.equals(Rules.KEYFILTER.name)) {
                    ret.put("dataType", "JSON");
                } else if (lastRule.equals(Rules.SAVEAS.name)) {
                    ret.put("dataType", "FIELD");
                } else {
                    ret.put("dataType", "STRING");
                }
            }

            return Response.ok().entity(ret).build();

        } catch (Exception e) {
            logger.error("Error encountered while run rules with parameter:{}", JSON.toJSONString(map), e);
            return Response.status(204).entity(new Result(-1, e.getMessage())).build();
        }
    }

    private Map<String, Object> getKafkaPlainLogContent(Map<String, Object> map) {
        String topic = map.get("kafkaTopic").toString();
        long offset = Long.parseLong(map.get("kafkaOffset").toString());
        long count = Long.parseLong(map.get("kafkaCount").toString());
        DbusDatasourceType dsType = DbusDatasourceType.parse(map.get("dsType").toString().toLowerCase());

        try {
            KafkaConsumer<String, String> consumer = PlainLogKafkaConsumerProvider.getKafkaConsumer();

            TopicPartition dataTopicPartition = new TopicPartition(topic, 0);
            List<TopicPartition> topics = Arrays.asList(dataTopicPartition);
            consumer.assign(topics);
            consumer.seek(dataTopicPartition, offset);

            List<List<String>> valuesList = new ArrayList<>();
            List<List<String>> keysList = new ArrayList<>();
            while(valuesList.size() < count) {
                ConsumerRecords<String, String> records = consumer.poll(3000);
                for (ConsumerRecord<String, String> record : records) {
                    if (dsType.equals(DbusDatasourceType.LOG_LOGSTASH)
                            || dsType.equals(DbusDatasourceType.LOG_LOGSTASH_JSON)) {
                        JSONObject value = JSON.parseObject(record.value());
                        for (String key : value.keySet()) {
                            if (value.get(key) instanceof String) continue;
                            value.put(key, JSON.toJSONString(value.get(key)));
                        }
                        ArrayList<String> values = new ArrayList<String>() {{
                            add(JSON.toJSONString(value));
                        }};
                        valuesList.add(values);
                    } else if(dsType.equals(DbusDatasourceType.LOG_FLUME) || dsType.equals(DbusDatasourceType.LOG_FILEBEAT) || dsType.equals(DbusDatasourceType.LOG_UMS)) {
                        ArrayList<String> values = new ArrayList<String>() {{
                            add(record.value());
                        }};
                        valuesList.add(values);
                    }

                    ArrayList<String> key = new ArrayList<String>() {{
                        add(record.key());
                    }};
                    keysList.add(key);

                    if(valuesList.size() >= count) break;
                }
                offset += records.count();
                consumer.seek(dataTopicPartition, offset);
                if(records.count() == 0) {
                    logger.warn("There is no content while reading kafka plain log");
                    break;
                }
            }
            Map<String, Object> ret = new HashMap<>();
            ret.put("keysList", keysList);
            ret.put("valuesList", valuesList);
            return ret;
        } catch (Exception e) {
            logger.error("Read kafka plain log error", e);
        }

        return null;
    }
    // 规则结束

    private Result validateTableStatus(DataTable table) throws Exception {
        DataTable waitingTable = null;
        List<DataTable> list = service.findTablesBySchemaID(table.getSchemaID());
        for (DataTable dataTable : list) {
            if (DataTable.WAITING.equals(dataTable.getStatus())) {
                waitingTable = dataTable;
                break;
            }
        }
        // 如果没有处于waiting状态的表,则返回成功
        if (waitingTable == null) return Result.OK;

        try {
            // 判断zk中节点的状态时间是否超过15分钟,超过则
            Result result = validateZookeeperNode(waitingTable);
            // 没有找到zookeeper相应的节点的情况返回100001
            if (result.getStatus() == 100001) {
                // 没有找到节点的情况
                boolean expire = System.currentTimeMillis() - waitingTable.getCreateTime().getTime() > 0.5 * 60 * 1000;
                return expire ? Result.OK : new Result(-1, String.format("table[%s] is waiting for initial-load", waitingTable.getTableName()));
            }
            if (result.getStatus() != 0) {
                return result;
            }
            return Result.OK;
        } catch (IllegalStateException e) {
            return new Result(-1, e.getMessage());
        }
    }

    private Result validateZookeeperNode(DataTable table) throws Exception {
        TableVersionService verService = TableVersionService.getService();
        TableVersion version = verService.findById(table.getVerID());
        Result result = new Result(0, "Ok");
        result.addParameter("waiting_table", table);
        if (version == null) {
            result.setStatus(-1);
            result.setMessage("Table version not found");
            return result;
        }

        byte[] data;
        String zkNode = Joiner.on("/").join("/DBus/FullPuller", table.getDsName(), table.getSchemaName(),
                table.getTableName(), version.getVersion());
        try {
            IZkService zk = ZookeeperServiceProvider.getInstance().getZkService();
            data = zk.getData(zkNode);
        } catch (KeeperException.NoNodeException e) {
            String info = String.format("zookeeper node [%s] not exists", zkNode);
            logger.warn(info);
            result.setStatus(100001);
            result.setMessage(info);
            return result; // 没有节点的情况下,要继续判断table保持waiting状态的时间
        }
        if (data != null && data.length > 0) {
            String json = new String(data, Charset.UTF8);
            InitialLoadStatus status = JSON.parseObject(json, InitialLoadStatus.class);
            if (Constants.FULL_PULL_STATUS_SPLITTING.equals(status.getStatus())) {
                String info = String.format("Table %s is waiting for loading data.", table.getTableName());
                logger.info(info);
                result.setStatus(-1);
                result.setMessage(info);
                return result;
            } else if (Constants.FULL_PULL_STATUS_PULLING.equals(status.getStatus())) {
                boolean expire = System.currentTimeMillis() - status.getUpdateTime().getTime() > 15 * 60 * 1000;
                if (expire) {
                    String info = String.format("Table[%s] data loading is running, but expired, last update time is %s",
                            table.getTableName(), status.getUpdateTime());
                    logger.info(info);
                    result.setMessage(info);
                    return result;
                }
                result.setStatus(-1);
                result.setMessage("Initial-load is running");
                return result;
            } else if (Constants.FULL_PULL_STATUS_ENDING.equals(status.getStatus())) {
                return result;
            } else {
                throw new IllegalStateException("Illegal state[" + status.getStatus() + "] of node " + zkNode);
            }
        }
        return result; // 要继续判断table保持waiting状态的时间
    }

    public static class ActiveTableParam {
        private String type;
        private int version;
        public static ActiveTableParam build(Map<String, String> map) {
            ActiveTableParam p = new ActiveTableParam();
            if(map == null) return p;

            if(map.containsKey("type")) {
                p.setType(map.get("type"));
            }
            if(map.containsKey("version")) {
                p.setVersion(Integer.parseInt(map.get("version")));
            }
            return p;
        }
        public void setType(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }

        public int getVersion() {
            return version;
        }

        public void setVersion(int version) {
            this.version = version;
        }
    }

    public static class InitialLoadStatus {
        private String status;
        @JSONField(format = "yyyy-MM-dd HH:mm:ss.SSS")
        private Date updateTime;

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public Date getUpdateTime() {
            return updateTime;
        }

        public void setUpdateTime(Date updateTime) {
            this.updateTime = updateTime;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println(System.currentTimeMillis());
        System.out.println(EncrypAES.decrypt("d3c945ef26564435eae2cbc4725fac9d"));
    }

    private static int getInt(Map<String, Object> map, String key) {
        return Integer.parseInt(map.get(key).toString());
    }
}
