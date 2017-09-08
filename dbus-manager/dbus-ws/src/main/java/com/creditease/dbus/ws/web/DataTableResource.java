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
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.enums.MessageEncodeType;
import com.creditease.dbus.utils.ControlMessageSender;
import com.creditease.dbus.ws.common.Charset;
import com.creditease.dbus.ws.common.HttpHeaderUtils;
import com.creditease.dbus.ws.common.Result;
import com.creditease.dbus.ws.domain.*;
import com.creditease.dbus.ws.mapper.*;
import com.creditease.dbus.ws.service.*;
import com.creditease.dbus.ws.service.mybatis.MybatisTemplate;
import com.creditease.dbus.ws.service.table.TableFetcher;
import com.creditease.dbus.ws.tools.ControlMessageSenderProvider;
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
import java.util.*;

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
            if("jsonlog".equals(dsType)) {
                dsType = "log";
            }
            String namespace = "";
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
                        versionsChangeHistory += ","+tableVersion.getVersion();
                    } catch (NumberFormatException e) {
                        logger.warn("transform verId to version failed for verid: {}, error message: {}",verId, e);
                    }
                }
                versionsChangeHistory = versionsChangeHistory.substring(1);
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

                if (DbusDatasourceType.ORACLE == dsType) {
                    logger.info("Activate oracle table");
                    // 处理oracle拉全量
                    ilService.oracleInitialLoadBySql(ds, table);
                } else if (DbusDatasourceType.MYSQL == dsType) {
                    logger.info("Activate mysql table");
                    // 处理mysql拉全量
                    ilService.mysqlInitialLoadBySql(ds, table);
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
               // DataSchemaService schemaService = DataSchemaService.getService();
               // List<DataSchema> tempList = schemaService.getDataSchemaByName(schemaName);
                TableFetcher fetcher = TableFetcher.getFetcher(ds);

                Map<String, Object> map = new HashMap<>();
                map.put("dsName", dsName);
                map.put("schemaName", schemaName);
                List<DataTable> list = fetcher.fetchTable(map);
                return Response.ok().entity(list).build();
                //return Response.ok(list.size()).build();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return Response.ok(Result.OK).build();
        }

    @GET
    @Path("/tablefield")
    public Response fetchTableField(@QueryParam("dsName") String dsName,@QueryParam("schemaName") String schemaName,@QueryParam("tableName") String tableName) {
        try {
            DataSourceService dsService = DataSourceService.getService();
            DbusDataSource ds = dsService.getDataSourceByName(dsName);
            TableFetcher fetcher = TableFetcher.getFetcher(ds);

            Map<String, Object> map = new HashMap<>();
            map.put("dsName", dsName);
            map.put("schemaName", schemaName);
            map.put("tableName", tableName);

            List<TableMeta> list = fetcher.fetchTableField(map);
            //System.out.print(list.size());

            return Response.ok().entity(list).build();
            //return Response.ok(list.size()).build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Response.ok(Result.OK).build();
    }

    @GET
    @Path("/searchRules")
    public Response searchRules(Map<String, Object> map) {
        try {
            MybatisTemplate template = MybatisTemplate.template();
            List<DataTableRule> result = template.query((session, args) -> {
                DataTableRuleMapper mapper = session.getMapper(DataTableRuleMapper.class);
                return mapper.getDataTableRule(Integer.parseInt(map.get("id").toString()));
            });
            return Response.ok().entity(result).build();
        } catch (Exception e) {
            logger.error("Error encountered while search rules information with parameter:{}", JSON.toJSONString(map), e);
            return Response.status(204).entity(new Result(-1, e.getMessage())).build();
        }
    }

    @GET
    @Path("/saveRules")
    public Response saveRules(Map<String, Object > map) {
        try {
            MybatisTemplate template = MybatisTemplate.template();
            template.query((session, args) -> {
                DataTableRuleMapper mapper = session.getMapper(DataTableRuleMapper.class);
                return mapper.deleteDataTableRule(Integer.parseInt(map.get("tableId").toString()));
            });
            if (map.get("rules") != null) {
                for (Map<String, Object> rule : (List<Map<String, Object>>) map.get("rules")) {

                    DataTableRule dataTableRule = new DataTableRule();
                    dataTableRule.setTableId(Integer.parseInt(rule.get("tableId").toString()));
                    dataTableRule.setOrder(Integer.parseInt(rule.get("order").toString()));
                    dataTableRule.setType(rule.get("type").toString());
                    dataTableRule.setColumn(rule.get("column").toString());
                    dataTableRule.setRe(rule.get("re").toString());

                    template.query((session, args) -> {
                        DataTableRuleMapper mapper = session.getMapper(DataTableRuleMapper.class);
                        return mapper.insertDataTableRule(dataTableRule);
                    });
                }
            }

            return Response.ok().build();
        } catch (Exception e) {
            logger.error("Error encountered while save rules with parameter:{}", JSON.toJSONString(map), e);
            return Response.status(204).entity(new Result(-1, e.getMessage())).build();
        }
    }

    @POST
    @Path("/executeSqlRule")
    public Response executeSqlRule(String param) {
        System.out.println(param);
        JSONObject map = JSON.parseObject(param);
        String sql = map.getString("sql");
        int length = map.getInteger("length");
        List<List<String>> data = new ArrayList<>();
        for(int i=0;i<length;i++) {
            data.add((List<String>)map.get("data["+i+"][]"));
        }

        try {
            Properties properties = PropertiesUtils.getProps("mybatis.properties");
            DbusDataSource source = new DbusDataSource();
            source.setDsType("mysql");
            source.setMasterURL(properties.get("url").toString());
            source.setDbusUser(properties.get("username").toString());
            source.setDbusPassword(properties.get("password").toString());
            TableFetcher fetcher = TableFetcher.getFetcher(source);
            List<List<String>> result = fetcher.doSqlRule(sql, data);
            return Response.ok().entity(result).build();
        } catch (Exception e) {
            logger.error("Error encountered while execute sql rule, error message:{}", e);
            return Response.status(204).entity(new Result(-1, e.getMessage())).build();
        }
    }

    @GET
    @Path("/readKafkaTopic")
    public Response readKafkaTopic(Map<String, Object > map) {
        try {
            Properties properties = PropertiesUtils.getProps("consumer.properties");
            properties.setProperty("client.id","readKafkaTopic");
            properties.setProperty("group.id","readKafkaTopic");
            //properties.setProperty("bootstrap.servers", "localhost:9092");
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
            String topic = map.get("topic").toString();
            //System.out.println("topic="+topic);
            TopicPartition topicPartition = new TopicPartition(topic, 0);
            List<TopicPartition> topics = Arrays.asList(topicPartition);
            consumer.assign(topics);
            consumer.seekToEnd(topics);
            long current = consumer.position(topicPartition);
            long end = current;
            current -= 1000;
            if(current < 0) current = 0;
            consumer.seek(topicPartition, current);
            List<String> result = new ArrayList<>();
            while (current < end) {
                //System.out.println("topic position = "+current);
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    result.add(record.value());
                    //System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                }
                current = consumer.position(topicPartition);
            }
            consumer.close();
            return Response.ok().entity(result).build();
        } catch (Exception e) {
            logger.error("Error encountered while readKafkaTopic with parameter:{}", JSON.toJSONString(map), e);
            return Response.status(204).entity(new Result(-1, e.getMessage())).build();
        }
    }

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
