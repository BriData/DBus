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

import com.creditease.dbus.commons.ControlMessage;
import com.creditease.dbus.utils.ControlMessageSender;
import com.creditease.dbus.ws.common.Result;
import com.creditease.dbus.ws.tools.ControlMessageSenderProvider;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.io.Console;
import java.util.Map;
import java.util.Properties;


/**
 * 提供topology的相关操作
 * Created by zhangyf on 16/9/6.s
 */
@Path("/ctlMessage")
@Consumes("application/json")
@Produces("application/json;charset=utf-8")
public class CtlMessageResource {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @POST
    @Path("send")
    public Response sendMessage(Map<String, String> map) {
        String topic = null;
        String strMessage = null;
        try {
            if (!validate(map)) {
                throw new Exception("参数验证失败");
            }
            strMessage = map.get("message");
            ControlMessageSender sender = ControlMessageSenderProvider.getInstance().getSender();
            ControlMessage message = ControlMessage.parse(strMessage);
            if(!validate(message)) {
                throw new Exception("消息验证失败");
            }

            topic = map.get("topic");
            sender.send(topic, message);
            logger.info("[control message] Send control message to topic[{}] success.\n {}", topic, strMessage);
            return Response.ok().entity(Result.OK).build();
            //return Response.status(200).entity(value).build();
        } catch (Exception e) {
            logger.error("[control message] Error encountered while sending control message.\ncontrol topic:{}\nmessage:{}", topic, strMessage, e);
            return Response.status(200).entity(new Result(-1, e.getMessage())).build();
        }
    }

    private boolean validate(Map<String, String> map) {
        if (map == null) return false;
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (StringUtils.isBlank(entry.getValue())) {
                return false;
            }
        }
        return true;
    }
    private boolean validate(ControlMessage message) {
        if(message.getId() <= 0) return false;
        if(StringUtils.isBlank(message.getType())) return false;
        if(StringUtils.isBlank(message.getFrom())) return false;
        if(StringUtils.isBlank(message.getTimestamp())) return false;
        return true;
    }
}
