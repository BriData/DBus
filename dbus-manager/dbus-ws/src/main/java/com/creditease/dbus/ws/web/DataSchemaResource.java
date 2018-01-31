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
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.ws.common.HttpHeaderUtils;
import com.creditease.dbus.ws.common.Result;
import com.creditease.dbus.ws.domain.DataSchema;
import com.creditease.dbus.ws.domain.DbusDataSource;
import com.creditease.dbus.ws.service.DataSchemaService;
import com.creditease.dbus.ws.service.DataSourceService;
import com.creditease.dbus.ws.service.schema.MongoSchemaFetcher;
import com.creditease.dbus.ws.service.schema.SchemaFetcher;
import com.github.pagehelper.PageInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.rmi.UnexpectedException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 提供DataSchema的相关操作
 */
@Path("/dataschemas")
@Consumes("application/json")
@Produces("application/json;charset=utf-8")
public class DataSchemaResource {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private DataSchemaService service = DataSchemaService.getService();

    @POST
    @Path("/search")
    public Response search(Map<String, Object> map) {
        try {
            PageInfo<DataSchema> result = service.search(getInt(map, "pageNum"), getInt(map, "pageSize"), map);
            return HttpHeaderUtils.allowCrossDomain(Response.ok()).entity(result).build();
        } catch (Exception e) {
            logger.error("Error encountered while search  dataSchema with parameter:{}", JSON.toJSONString(map), e);
            return HttpHeaderUtils.allowCrossDomain(Response.status(200)).entity(new Result(-1, e.getMessage())).build();
        }
    }

    @POST
    @Path("/searchAll")
    public Response searchAll() {
        List<DataSchema> list = service.findAllAll();
        return Response.status(200).entity(list).build();
    }

    private static int getInt(Map<String, Object> map, String key) {
        return Integer.parseInt(map.get(key).toString());
    }



    /**
     * 根据id获取执行的DataSchema
     * @param id DataSchema的ID
     * @return id所代表的DataSchema对象
     */
    @GET
    @Path("/{id:[0-9]+}")
    public Response findById(@PathParam("id") Long id) {
        DataSchema dataSchema = service.getDataSchemaById(id);
        return Response.status(200).entity(dataSchema).build();
    }

    /**
     * 根据dataSchema名称获取dataSchema列表
     * @param name 如果name不存在则返回全部数据源
     * @return dataSchema列表
     */
    @GET
    @Path("{name}")
    public Response findDataSchema(@PathParam("name") String name) {
        List<DataSchema> list = service.findDataSchemas(name);
        return Response.status(200).entity(list).build();
    }

    @GET
    @Path("/checkschema")
    public Response checkDataSchema(@QueryParam("dsId") Long dsId,@QueryParam("schemaName") String schemaName) {
        List<DataSchema> list = service.checkDataSchema(dsId,schemaName);
        return Response.status(200).entity(list).build();
    }

    /**
     * 说明：不改动以前接口，这个接口和上一个类似。
     */
   /*
    @GET
    @Path("/schema/{schemaName}")
    public Response findSchemaBySchemaName(@PathParam("schemaName") String schemaName) {
        List<DataSchema> list = service.findDataSchemas(schemaName);
        return Response.status(200).entity(list).build();
    }*/


    /**
     * 根据dataSource ID获取dataSchema列表
     * @return dataSchema列表
     */
    @GET
    public Response findByDsId(@QueryParam("dsId") Long dsId) {
        List<DataSchema> list = service.findByDsId(dsId);
        return HttpHeaderUtils.allowCrossDomain(Response.status(200)).entity(list).build();
    }

    /**
     * 根据dataSource ID获取dataSchema列表
     * @return dataSchema列表
     */
    @GET
    @Path("/dsId")
    public Response findSchemasByDsId(@QueryParam("dsId") Long dsId) {
        List<DataSchema> list = service.findByDsId(dsId);
        return HttpHeaderUtils.allowCrossDomain(Response.status(200)).entity(list).build();
    }

    @POST
    @Path("/update")
    public Response updateDataSchema(DataSchema  ds) {
        try {
            int i = service.updateDataSchema(ds);
            System.out.print(i);
            return Response.ok().entity(Result.OK).build();
        } catch (Exception ex) {
            logger.error("Error encountered while update DateSource with parameter {id:{}, ds:{}}",ds, ex);
            return Response.status(200).entity(new Result(-1, ex.getMessage())).build();
        }
    }

    /**
     * 新建数据源
     * @param ds DataSchema
     * @return 返回新建状态
     */
    @POST
    public Response insertDataSchema(DataSchema ds) {
        try {
            service.insertDataSchema(ds);
            long schemaId = ds.getId();
            DataSchema dataSchema = service.getDataSchemaById(schemaId);
            return Response.ok().entity(schemaId).build();
        } catch (Exception ex) {
            logger.error("Error encountered while create DateSource with parameter:{}", ds, ex);
            return Response.status(200).entity(new Result(-1, ex.getMessage())).build();
        }
    }

    /**
     * 激活指定的DataSchema,使该DataSchema生效
     * @param id DataSchema ID
     */
    @PUT
    @Path("{id:[1-9]+}/status")
    public Response activateDataSchema(@PathParam("id")long id) {
        try {
            service.activateDataSchema(id);
            return Response.ok().entity(Result.OK).build();
        } catch (Exception ex) {
            logger.error("Error encountered while active DateSource with parameter:{}", id, ex);
            return Response.status(200).entity(new Result(-1, ex.getMessage())).build();
        }
    }

    /**
     * 使DataSchema无效
     * @param id DataSchema ID
     */
    @DELETE
    @Path("{id:[1-9]+}/status")
    public Response deactivateDataSchema(@PathParam("id")long id) {
        try {
            service.deactivateDataSchema(id);
            return Response.ok().entity(Result.OK).build();
        } catch (Exception ex) {
            logger.error("Error encountered while deactivate DateSource with parameter:{}", id, ex);
            return Response.status(200).entity(new Result(-1, ex.getMessage())).build();
        }
    }

    @GET
    @Path("/schemaname")
    public Response fetchSchema(@QueryParam("dsName") String dsName) {
        try {
            DataSourceService dsService = DataSourceService.getService();
            DbusDataSource ds = dsService.getDataSourceByName(dsName);
            List<DataSchema> list;
            if(DbusDatasourceType.stringEqual(ds.getDsType(),DbusDatasourceType.MYSQL)
                || DbusDatasourceType.stringEqual(ds.getDsType(),DbusDatasourceType.ORACLE))
            {
                SchemaFetcher fetcher = SchemaFetcher.getFetcher(ds);
                list = fetcher.fetchSchema();
            } else if(DbusDatasourceType.stringEqual(ds.getDsType(),DbusDatasourceType.MONGO)) {
                MongoSchemaFetcher fetcher = new MongoSchemaFetcher(ds);
                list = fetcher.fetchSchema();
            } else {
                throw new IllegalArgumentException("Unsupported datasource type");
            }
            for(int i=0;i<list.size();i++){
                list.get(i).setDsId(ds.getId());
                list.get(i).setStatus(ds.getStatus());
                list.get(i).setSrcTopic(ds.getDsName()+"."+list.get(i).getSchemaName());
                list.get(i).setTargetTopic(ds.getDsName()+"."+list.get(i).getSchemaName()+".result");
            }
            return Response.ok().entity(list).build();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return Response.ok(Result.OK).build();
    }

    @GET
    @Path("/deleteSchema")
    public Response deleteSchema(Map<String, Object> map) {
        int schemaId = Integer.parseInt(map.get("schemaId").toString());
        int result = service.deleteSchema(schemaId);
        return Response.ok().entity(result).build();
    }
}
