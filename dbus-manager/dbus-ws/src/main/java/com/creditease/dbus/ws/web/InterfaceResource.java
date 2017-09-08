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
import com.creditease.dbus.ws.common.HttpHeaderUtils;
import com.creditease.dbus.ws.common.Result;
import com.creditease.dbus.ws.domain.InterfaceInfo;
import com.creditease.dbus.ws.service.InterfaceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;


@Path("/interface")
@Consumes("application/json")
@Produces("application/json;charset=utf-8")

public class InterfaceResource {
    @GET
    @Path("/search")
    public Response searchInfo() {
        try {
            InterfaceService service = InterfaceService.getService();
            List<InterfaceInfo> result = service.searchInfo();
            return Response.ok().entity(result).build();
        }catch (Exception e) {
            return Response.status(200).entity(new Result(-1, e.getMessage())).build();
        }
    }
}
