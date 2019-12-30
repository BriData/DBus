/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2019 Bridata
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


package com.creditease.dbus.stream.common.appender.spout.processor;

import com.creditease.dbus.commons.ControlMessage;
import com.creditease.dbus.commons.CtlMessageResult;
import com.creditease.dbus.commons.CtlMessageResultSender;

/**
 * Created by zhangyf on 16/12/23.
 */
public class CtrlMessagePostOperation {
    private String zkconnect;

    public static CtrlMessagePostOperation create(String zkconnect) {
        return new CtrlMessagePostOperation(zkconnect);
    }

    private CtrlMessagePostOperation(String zkconnect) {
        this.zkconnect = zkconnect;
    }

    public void spoutReloaded(ControlMessage message) {
        CtlMessageResult result = new CtlMessageResult("appender-spout", "appender reloaded successful!");
        result.setOriginalMessage(message);
        CtlMessageResultSender sender = new CtlMessageResultSender(message.getType(), zkconnect);
        sender.send("appender-spout", result, true, true);
    }

    public void spoutResumed(ControlMessage message, String resultMsg) {
        CtlMessageResult result = new CtlMessageResult("appender-spout", resultMsg);
        result.setOriginalMessage(message);
        CtlMessageResultSender sender = new CtlMessageResultSender(message.getType(), zkconnect);
        sender.send("appender-spout", result, true, true);
    }
}
