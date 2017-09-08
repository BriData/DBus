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
import com.creditease.dbus.ws.common.Result;
import com.creditease.dbus.ws.domain.StormTopology;
import com.creditease.dbus.ws.service.TopologyService;
import com.github.pagehelper.PageInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.util.Map;

/**
 * 提供topology的相关操作
 * Created by zhangyf on 16/9/6.s
 */
@Path("/topologies")
@Consumes("application/json")
@Produces("application/json;charset=utf-8")
public class TopologyResource {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @GET
    public Response topologyList(@QueryParam("pageNum") int pageNum, @QueryParam("pageSize") int pageSize, @QueryParam("dsId")Long dsId) {
        try {
            TopologyService service = TopologyService.getService();
            PageInfo<StormTopology> result = service.search(pageNum, pageSize, dsId);
            return Response.ok().entity(result).build();
        } catch (Exception e) {
            logger.error("Error encountered while list topologies with parameter:{}", dsId, e);
            return Response.status(200).entity(new Result(-1, e.getMessage())).build();
        }
    }

    @POST
    public Response addTopology(StormTopology st) {
        try {
            TopologyService service = TopologyService.getService();
            int result = service.insertTopology(st);
            return Response.ok().entity(result).build();
        } catch (Exception e) {
            logger.error("Error encountered while list topologies with parameter:{}", JSON.toJSONString(st), e);
            return Response.status(200).entity(new Result(-1, e.getMessage())).build();
        }
    }
}
