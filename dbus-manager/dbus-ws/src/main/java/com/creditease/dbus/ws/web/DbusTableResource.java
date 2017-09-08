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

import com.creditease.dbus.ws.common.Result;
import com.creditease.dbus.ws.domain.DataTable;
import com.creditease.dbus.ws.domain.DbusDataSource;
import com.creditease.dbus.ws.service.DataSourceService;
import com.creditease.dbus.ws.service.tableSource.TableFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Path("/dbustable")
@Consumes("application/json")
@Produces("application/json;charset=utf-8")
public class DbusTableResource {
    @POST
    @Path("/tableinsert")
    public Response tableInsert(@QueryParam("dsName") String dsName,@QueryParam("schemaName") String schemaName,@QueryParam("tableName") String tableName) {
        try {
            DataSourceService dsService = DataSourceService.getService();
            DbusDataSource ds = dsService.getDataSourceByName(dsName);
            String dsType = ds.getDsType();
            if("mysql".equals(dsType))
                return Response.ok().entity(Result.OK).build();

            TableFetcher fetcher = TableFetcher.getFetcher(ds);
            Map<String, Object> map = new HashMap<>();
            map.put("schemaName",schemaName);
            map.put("tableName",tableName);
            int  t = fetcher.insertTable(map);
            return Response.ok().entity(t).build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Response.ok(Result.OK).build();
    }

    @GET
    @Path("/listtable")
    public Response listTable(@QueryParam("dsName") String dsName) {
        try {
            DataSourceService dsService = DataSourceService.getService();
            DbusDataSource ds = dsService.getDataSourceByName(dsName);
            String dsType = ds.getDsType();
            if("mysql".equals(dsType))
                return Response.ok().entity(Result.OK).build();
            TableFetcher fetcher = TableFetcher.getFetcher(ds);
            List<DataTable> list = fetcher.listTable();
            return Response.ok().entity(list).build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Response.ok(Result.OK).build();
    }


}
