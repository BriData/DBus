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
import com.creditease.dbus.ws.domain.*;
import com.creditease.dbus.ws.service.FullpullHistoryService;
import com.github.pagehelper.PageInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.util.*;

/**
 * 数据源resource,提供数据源的相关操作
 */
@Path("/fullpullHistory")
@Consumes("application/json")
@Produces("application/json;charset=utf-8")
public class FullpullHistoryResource {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private FullpullHistoryService service = FullpullHistoryService.getService();


    /**
     * 根据数据源名称,schema名称以及table名称获取相关table列表
     * @return table列表
     */

    @POST
    @Path("/search")
    public Response searchDataTable(Map<String, Object> map) {
        try {
            PageInfo<FullPullHistory> result = service.search(Integer.parseInt(map.get("pageNum").toString()), Integer.parseInt(map.get("pageSize").toString()), map);
            return Response.ok().entity(result).build();
        } catch (Exception e) {
            logger.error("Error encountered while search fullpulll history with parameter:{}", JSON.toJSONString(map), e);
            return Response.status(200).entity(new Result(-1, e.getMessage())).build();
        }
    }
}
