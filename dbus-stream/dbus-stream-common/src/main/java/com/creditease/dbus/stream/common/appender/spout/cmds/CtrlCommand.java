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


package com.creditease.dbus.stream.common.appender.spout.cmds;

import com.creditease.dbus.commons.ControlMessage;
import com.creditease.dbus.stream.common.appender.enums.Command;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Shrimp on 16/6/21.
 */
public abstract class CtrlCommand {
    private static Logger logger = LoggerFactory.getLogger(CtrlCommand.class);

    private static CtrlCommand unknownCmd = new NoOpCmd();

    public abstract <T> T execCmd(CommandHandler handler, Object... args);

    public static CtrlCommand parse(Command command, ControlMessage message) {
        CtrlCommand retCmd;
        switch (command) {
            case APPENDER_TOPIC_RESUME:

                TopicResumeCmd cmd = new TopicResumeCmd();
                cmd.setDatasource(message.payloadValue("DBUS_DATASOURCE_ID", String.class));
                cmd.setSchema(message.payloadValue("SCHEMA_NAME", String.class));
                cmd.setTable(message.payloadValue("TABLE_NAME", String.class));
                cmd.setStatus(message.payloadValue("STATUS", String.class));
                cmd.setCommandSender(message.getFrom());
                cmd.setTopic(message.payloadValue("topic", String.class));
                try {
                    cmd.setBatchId(Integer.parseInt(message.payloadValue("BATCH_NO", String.class)));
                    logger.info("Get batch_id new value: {}", cmd.getBatchId());
                } catch (Exception e) {
                    logger.error("batch_id not found from control message. message:{}", message.toJSONString());
                }
                //if(!DataTable.STATUS_ABORT.equals(cmd.getStatus())) {
                //cmd.setVersion(message.payloadValue("VERSION", Integer.class));
                //}

                retCmd = cmd;
                if (Strings.isNullOrEmpty(cmd.getSchema())) {
                    logger.error("Control message Error, no SCHEMA_NAME found in payload {}", message.toJSONString());
                    retCmd = unknownCmd;
                }
                if (Strings.isNullOrEmpty(cmd.getTable())) {
                    logger.error("Control message Error, no TABLE_NAME found in payload {}", message.toJSONString());
                    retCmd = unknownCmd;
                }
                if (Strings.isNullOrEmpty(cmd.getStatus())) {
                    logger.error("Control message Error, no STATUS found in payload {}", message.toJSONString());
                    retCmd = unknownCmd;
                }
                return retCmd;
            default:
                return unknownCmd;
        }
    }

    public static CtrlCommand parse(ControlMessage message) {
        return parse(Command.parse(message.getType()), message);
    }
}
