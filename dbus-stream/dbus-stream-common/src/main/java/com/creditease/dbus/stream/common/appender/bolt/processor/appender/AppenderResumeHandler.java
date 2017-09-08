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

package com.creditease.dbus.stream.common.appender.bolt.processor.appender;

import com.creditease.dbus.commons.ControlMessage;
import com.creditease.dbus.commons.CtlMessageResult;
import com.creditease.dbus.commons.CtlMessageResultSender;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandlerHelper;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.CommandHandlerListener;
import com.creditease.dbus.stream.common.appender.cache.ThreadLocalCache;
import com.creditease.dbus.stream.common.appender.enums.Command;
import com.creditease.dbus.stream.common.appender.bean.DataTable;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.spout.cmds.TopicResumeCmd;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Shrimp on 16/7/1.
 */
public class AppenderResumeHandler implements BoltCommandHandler {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private CommandHandlerListener listener;

    public AppenderResumeHandler(CommandHandlerListener listener) {
        this.listener = listener;
    }

    @Override
    public void handle(Tuple input) {
        EmitData emitData = (EmitData) input.getValueByField(Constants.EmitFields.DATA);
        TopicResumeCmd ctrlCmd = emitData.get(EmitData.CTRL_CMD);
        String msg = null;
        try {
            BoltCommandHandlerHelper.changeDataTableStatus(ctrlCmd.getSchema(), ctrlCmd.getTable(), ctrlCmd.getStatus());
            String key = Utils.buildDataTableCacheKey(ctrlCmd.getSchema(), ctrlCmd.getTable());
            // 修改data table的status字段
            DataTable dataTable = ThreadLocalCache.get(Constants.CacheNames.DATA_TABLES, key);
            if(ctrlCmd.getBatchId() != 0) {
                dataTable.setBatchId(ctrlCmd.getBatchId());
                logger.info("Set {}.batchId to newValue : {}", dataTable.getSchema()+"."+dataTable.getTableName(),dataTable.getBatchId());
            } else {
                logger.error("batch_id not found.");

            }
// 全量不再修改version，这里不需要修改内存中的version值
//            if(ctrlCmd.getVersion() != 0) {
//                // 只需要修改缓存,数据库由拉全量端修改
//                MetaVersion ver = MetaVerController.getVersionFromCache(ctrlCmd.getSchema(), ctrlCmd.getTable());
//                logger.info("Table {} version from {} to {}", Joiner.on(".").join(ver.getSchema(), ver.getTable()), ver.getVersion(), ctrlCmd.getVersion());
//                ver.setVersion(ctrlCmd.getVersion());
//            }
            msg = "table status changed to "+ ctrlCmd.getStatus();
        } catch (IllegalArgumentException e) {
            msg = e.getMessage();
            logger.error(e.getMessage(), e);
        } finally {
            ControlMessage message = emitData.get(EmitData.MESSAGE);
            CtlMessageResult result = new CtlMessageResult("appender-bolt", msg);
            result.setOriginalMessage(message);
            CtlMessageResultSender sender = new CtlMessageResultSender(message.getType(), listener.getZkconnect());
            sender.send("appender", result, false, true);
        }

        /**
         * 需要发送给WrapperBolt
         * 重置zk，重新获取UMS_UID
         */
        this.emit(listener.getOutputCollector(), input, (String)input.getValueByField(Constants.EmitFields.GROUP_FIELD), emitData, Command.APPENDER_TOPIC_RESUME);
    }
}
