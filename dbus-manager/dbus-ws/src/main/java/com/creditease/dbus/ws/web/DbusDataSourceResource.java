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
import com.creditease.dbus.commons.EncrypAES;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.ws.common.HttpHeaderUtils;
import com.creditease.dbus.ws.common.Result;
import com.creditease.dbus.ws.domain.DbusDataSource;
import com.creditease.dbus.ws.service.DataSourceService;
import com.creditease.dbus.ws.service.source.MongoSourceFetcher;
import com.creditease.dbus.ws.service.source.SourceFetcher;
import com.github.pagehelper.PageInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
/**
 * 数据源resource,提供数据源的相关操作
 * Created by dongwang47 on 2016/8/29.
 */
@Path("/datasources")
@Consumes("application/json")
@Produces("application/json;charset=utf-8")
public class DbusDataSourceResource {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private DataSourceService service = DataSourceService.getService();

    /**
     * 根据id获取执行的数据源
     * @param id 数据源ID
     * @return id所代表的数据源对象
     */
    @GET
    @Path("/{id:[0-9]+}")
    public Response findById(@PathParam("id") Long id) {
        DbusDataSource dataSource = service.getDataSourceById(id);
        //encryp(dataSource);
        return Response.status(200).entity(dataSource).build();
    }

    /**
     * 根据数据源名称获取数据源列表
     * @param name 如果name不存在则返回全部数据源
     * @return 数据源列表
     */
    @GET
    public Response findDataSources(@QueryParam("name") String name) {
        List<DbusDataSource> list = service.findDataSources(name);
        list.stream().forEach(ds->encryp(ds));
        return HttpHeaderUtils.allowCrossDomain(Response.status(200)).entity(list).build();
    }

    @GET
    @Path("/checkName")
    public Response findDataSources(Map<String,Object> map) {
        List<DbusDataSource> list = service.findDataSources(String.valueOf(map.get("dsName")));
        list.stream().forEach(ds->encryp(ds));
        return HttpHeaderUtils.allowCrossDomain(Response.status(200)).entity(list).build();
    }


    @GET
    @Path("/validate")
    public Response validateDataSources(Map<String,Object> map){
        try {
            DbusDatasourceType dsType = DbusDatasourceType.parse(map.get("dsType").toString());
            if(dsType.equals(DbusDatasourceType.LOG_LOGSTASH)
                    || dsType.equals(DbusDatasourceType.LOG_LOGSTASH_JSON)
                    || dsType.equals(DbusDatasourceType.LOG_UMS)
                    || dsType.equals(DbusDatasourceType.LOG_FLUME)
                    || dsType.equals(DbusDatasourceType.LOG_FILEBEAT)) {
                return Response.ok().entity(1).build();
            }
            else if(dsType.equals(DbusDatasourceType.MYSQL)
                    || dsType.equals(DbusDatasourceType.ORACLE)){
                SourceFetcher fetcher = SourceFetcher.getFetcher(map);
                int temp = fetcher.fetchSource(map);
                return Response.ok().entity(temp).build();
            } else if(dsType.equals(DbusDatasourceType.MONGO)) {
                MongoSourceFetcher fetcher = new MongoSourceFetcher();
                int temp = fetcher.fetchSource(map);
                return Response.ok().entity(temp).build();
            }
            return Response.ok().entity(0).build();

        } catch (Exception e) {
            logger.error("Error encountered while validate datasources with parameter:{}", JSON.toJSONString(map), e);
            return Response.status(200).entity(new Result(-1, e.getMessage())).build();
        }
    }

    @GET
    @Path("/searchFromSource")
    public Response searchFromSource(Map<String,Object> map){
        try {
            SourceFetcher fetcher = SourceFetcher.getFetcher(map);
            List list = fetcher.fetchTableStructure(map);
            return Response.ok().entity(list).build();
        } catch (Exception e) {
            logger.error("Error encountered while validate datasources with parameter:{}", JSON.toJSONString(map), e);
            return Response.status(200).entity(new Result(-1, e.getMessage())).build();
        }
    }


    /**
     * 初始化ds页面table
     * @return 数据源列表
     */
    @POST
    @Path("/search")
    public Response searchDataSources(Map<String, Object> map) {
        try {
            DataSourceService service = DataSourceService.getService();
            PageInfo<DbusDataSource> result = service.search(getInt(map, "pageNum"), getInt(map, "pageSize"), map);
            return Response.ok().entity(result).build();
        } catch (Exception e) {
            logger.error("Error encountered while search datasources with parameter:{}", JSON.toJSONString(map), e);
            return Response.status(200).entity(new Result(-1, e.getMessage())).build();
        }
    }


    @POST
    @Path("/update")
    public Response updateDataSource(DbusDataSource ds) {
        try {
            service.updateDataSource(ds);
            return Response.ok().entity(Result.OK).build();
        } catch (Exception ex) {
            logger.error("Error encountered while update DateSource with parameter {id:{}, ds:{}}", ds, ex);
            return Response.status(200).entity(new Result(-1, ex.getMessage())).build();
        }
    }

    /**
     * 新建数据源
     * @param ds 数据
     * @return 返回新建状态
     */
    @POST
    public Response insertDataSource(DbusDataSource ds) {
        try {
            service.insertDataSource(ds);
            Long temp = ds.getId();
            return Response.ok().entity(temp).build();
        } catch (Exception ex) {
            logger.error("Error encountered while create DateSource with parameter:{}", ds, ex);
            return Response.status(200).entity(new Result(-1, ex.getMessage())).build();
        }
    }

    /**
     * 激活指定的数据源,使该数据源生效
     * @param id 数据源ID
     */
    @PUT
    @Path("{id:[1-9]+}/status")
    public Response activateDataSource(@PathParam("id")long id) {
        try {
            service.activateDataSource(id);
            return Response.ok().entity(Result.OK).build();
        } catch (Exception ex) {
            logger.error("Error encountered while active DateSource with parameter:{}", id, ex);
            return Response.status(200).entity(new Result(-1, ex.getMessage())).build();
        }
    }

    /**
     * 使数据源无效
     * @param id 数据源ID
     */
    @DELETE
    @Path("{id:[1-9]+}/status")
    public Response deactivateDataSource(@PathParam("id")long id) {
        try {
            service.deactivateDataSource(id);
            return Response.ok().entity(Result.OK).build();
        } catch (Exception ex) {
            logger.error("Error encountered while deactivate DateSource with parameter:{}", id, ex);
            return Response.status(200).entity(new Result(-1, ex.getMessage())).build();
        }
    }

    @GET
    @Path("/deleteDataSource")
    public Response deleteDataSource(Map<String, Object> map) {
        int dsId = Integer.parseInt(map.get("dsId").toString());
        int result = service.deleteDataSourceById(dsId);
        return Response.ok().entity(result).build();
    }
    
    private static void encryp(DbusDataSource ds) {
    	if(ds != null && ds.getDbusPassword() != null) {
    		ds.setDbusPassword(EncrypAES.encrypt(ds.getDbusPassword()));
    	}
    }
    
    public static void main(String[] args) {
        System.out.println(EncrypAES.decrypt(EncrypAES.encrypt("123")));
    }

    private static int getInt(Map<String, Object> map, String key) {
        return Integer.parseInt(map.get(key).toString());
    }
}
