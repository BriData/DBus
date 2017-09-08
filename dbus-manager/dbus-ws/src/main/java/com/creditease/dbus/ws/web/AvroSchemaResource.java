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
import com.creditease.dbus.ws.domain.AvroSchema;
import com.creditease.dbus.ws.service.AvroSchemaService;
import com.github.pagehelper.PageInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.util.Map;

/**
 * 提供Avro Schema的相关操作
 * Created by zhangyf on 16/9/6.s
 */
@Path("/avroSchemas")
@Consumes("application/json")
@Produces("application/json;charset=utf-8")
public class AvroSchemaResource {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @POST
    @Path("/search")
    public Response search(Map<String, Object> map) {
        try {
            AvroSchemaService service = AvroSchemaService.getService();
            PageInfo<AvroSchema> result = service.search(getInt(map, "pageNum"), getInt(map, "pageSize"), map);
            return Response.ok().entity(result).build();
        } catch (Exception e) {
            logger.error("Error encountered while search avro schema with parameter:{}", JSON.toJSONString(map), e);
            return Response.status(200).entity(new Result(-1, e.getMessage())).build();
        }
    }

    private static int getInt(Map<String, Object> map, String key) {
        return Integer.parseInt(map.get(key).toString());
    }
}
