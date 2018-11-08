/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2018 Bridata
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

package com.creditease.dbus.stream.appender.bolt;

import com.creditease.dbus.commons.ControlMessage;
import com.creditease.dbus.commons.CtlMessageResult;
import com.creditease.dbus.commons.CtlMessageResultSender;
import com.creditease.dbus.commons.PropertiesHolder;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.stream.appender.exception.InitializationException;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.Constants.EmitFields;
import com.creditease.dbus.stream.common.Constants.StormConfigKey;
import com.creditease.dbus.stream.common.appender.bean.DbusDatasource;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandlerProvider;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltHandlerManager;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.CommandHandlerListener;
import com.creditease.dbus.stream.common.appender.cache.GlobalCache;
import com.creditease.dbus.stream.common.appender.cache.ThreadLocalCache;
import com.creditease.dbus.stream.common.appender.enums.Command;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.Map;

/**
 * 1) 用于处理schema topic的信息
 * 2) 用于处理meta信息同步
 * 3) 用于将message与schema/meta/version进行关联
 * Created by Shrimp on 16/6/2.
 */
public class DbusAppenderBolt extends BaseRichBolt implements CommandHandlerListener {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private String topologyId;
    private String zkConnect;
    private String datasource;
    private String zkRoot;
    private boolean initialized = false;
    private OutputCollector collector;
    private TopologyContext context;
    private BoltHandlerManager handlerManager;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.context = context;

        if (!initialized) {
            this.topologyId = (String) conf.get(StormConfigKey.TOPOLOGY_ID);
            this.datasource = (String) conf.get(StormConfigKey.DATASOURCE);
            this.zkConnect = (String) conf.get(StormConfigKey.ZKCONNECT);
            this.zkRoot = Utils.buildZKTopologyPath(topologyId);
            // 初始化配置文件
            try {
                PropertiesHolder.initialize(zkConnect, zkRoot);
                GlobalCache.initialize(datasource);
                handlerManager = new BoltHandlerManager(buildProvider());
                reloadBolt(null);
                logger.info(getClass().getName() + " Initialized!");
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                throw new InitializationException(e);
            }
            initialized = true;
        }
    }

    @Override
    public void execute(Tuple input) {
        if (TupleUtils.isTick(input)) {
            collector.ack(input);
            return;
        }
        try {
            Command cmd = (Command) input.getValueByField(EmitFields.COMMAND);
            BoltCommandHandler handler = handlerManager.getHandler(cmd);
            handler.handle(input);
            this.collector.ack(input);
        } catch (Exception e) {
            this.collector.fail(input);
            this.collector.reportError(e);
            logger.error("Process Error!", e);
        }

    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(EmitFields.GROUP_FIELD, EmitFields.DATA, EmitFields.COMMAND, EmitFields.EMIT_TYPE));
    }

    @Override
    public OutputCollector getOutputCollector() {
        return collector;
    }

    @Override
    public void reloadBolt(Tuple tuple) {
        String msg = null;
        try {
            PropertiesHolder.reload();
            Command.initialize();
            ThreadLocalCache.reload();
            msg = "appender bolt reload successful!";
            logger.info("Appender bolt was reloaded at:{}", System.currentTimeMillis());
        } catch (Exception e) {
            msg = e.getMessage();
            throw new RuntimeException(e);
        } finally {
            if (tuple != null) {
                EmitData data = (EmitData) tuple.getValueByField(Constants.EmitFields.DATA);
                ControlMessage message = data.get(EmitData.MESSAGE);
                CtlMessageResult result = new CtlMessageResult("appender-bolt", msg);
                result.setOriginalMessage(message);
                CtlMessageResultSender sender = new CtlMessageResultSender(message.getType(), zkConnect);
                sender.send("appender-bolt-" + context.getThisTaskId(), result, false, true);
            }
        }
    }

    @Override
    public String getZkconnect() {
        return zkConnect;
    }

    private BoltCommandHandlerProvider buildProvider() throws Exception {
        /*
        Set<Class<?>> classes = AnnotationScanner.scan("com.creditease.dbus", BoltCmdHandlerProvider.class, (clazz, annotation) -> {
            BoltCmdHandlerProvider ca = clazz.getAnnotation(BoltCmdHandlerProvider.class);
            return ca.type() == BoltType.APPENDER_BOLT;
        });
        if (classes.size() <= 0) throw new ProviderNotFoundException();
        if (classes.size() > 1) {
            String providers = "";
            for (Class<?> clazz : classes) {
                providers += clazz.getName() + " ";
            }
            throw new RuntimeException("too many providers found: " + providers);
        }
        */
        DbusDatasourceType type = GlobalCache.getDatasourceType();
        DbusDatasource ds = GlobalCache.getDatasource();
        String name;
        if (type == DbusDatasourceType.ORACLE) {
            name = "com.creditease.dbus.stream.oracle.appender.bolt.processor.provider.AppenderCmdHandlerProvider";
        } else if (type == DbusDatasourceType.MYSQL) {
            name = "com.creditease.dbus.stream.mysql.appender.bolt.processor.provider.AppenderCmdHandlerProvider";
        }
        else {
            throw new IllegalArgumentException("Illegal argument [" + type.toString() + "] for building BoltCommandHandler map!");
        }
        Class<?> clazz = Class.forName(name);
        Constructor<?> constructor = clazz.getConstructor(CommandHandlerListener.class);
        return (BoltCommandHandlerProvider) constructor.newInstance(this);
    }
}
