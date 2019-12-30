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


package com.creditease.dbus.stream.common.appender.bolt.processor.appender;

import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.DataTable;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.bean.MetaVersion;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandlerHelper;
import com.creditease.dbus.stream.common.appender.bolt.processor.MetaVerController;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.CommandHandlerListener;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Shrimp on 16/7/1.
 */
public class AppenderInitialLoadHandler implements BoltCommandHandler {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private CommandHandlerListener listener;

    public AppenderInitialLoadHandler(CommandHandlerListener listener) {
        this.listener = listener;
    }

    @Override
    public void handle(Tuple tuple) {
        EmitData emitData = (EmitData) tuple.getValueByField(Constants.EmitFields.DATA);
        String dbschema = emitData.get(EmitData.DB_SCHEMA);
        String table = emitData.get(EmitData.DATA_TABLE);

        logger.info("Full data request was received [{}.{}]", dbschema, table);

        MetaVersion ver = MetaVerController.getVersionFromCache(dbschema, table);

        if (ver == null) {
            throw new RuntimeException("The version of table " + dbschema + "." + table + " was not found!");
        }

        //EmitData data = new EmitData();
        //data.add(EmitData.AVRO_SCHEMA, EmitData.NO_VALUE);
        //data.add(EmitData.VERSION, ver);
        //DbusMessage message = BoltCommandHandlerHelper.buildTerminationMessage(dbschema, table, ver.getVersion());
        //data.add(EmitData.MESSAGE, message);

        // 修改data table表状态
        BoltCommandHandlerHelper.changeDataTableStatus(ver.getSchema(), ver.getTable(), DataTable.STATUS_WAITING);

        //List<Object> values = new Values(EmitData.NO_VALUE, data, Command.DATA_INCREMENT_TERMINATION);
        //this.listener.getOutputCollector().emit(tuple, values);
        //this.emit(listener.getOutputCollector(),tuple, groupField(dbschema, table), data, Command.DATA_INCREMENT_TERMINATION);


        logger.info("Full data request was processed");
    }
}
