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

import com.creditease.dbus.commons.*;
//import com.creditease.dbus.msgencoder.ExternalEncoders;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.msgencoder.PluginManagerProvider;
import com.creditease.dbus.stream.appender.exception.InitializationException;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.Constants.EmitFields;
import com.creditease.dbus.stream.common.Constants.StormConfigKey;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.bolt.processor.AppenderPluginLoader;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandlerProvider;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltHandlerManager;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.CommandHandlerListener;
import com.creditease.dbus.stream.common.appender.cache.GlobalCache;
import com.creditease.dbus.stream.common.appender.cache.ThreadLocalCache;
import com.creditease.dbus.stream.common.appender.enums.Command;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Properties;


public class WrapperBolt extends BaseRichBolt implements CommandHandlerListener {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private String zkRoot;
    private String topologyId;
    private String datasource;
    private boolean initialized = false;

    private OutputCollector collector;
    private BoltHandlerManager handlerManager;
    private Producer<String, String> producer;
    private TopologyContext context;
    private String zkconnect;
    private ZkService zkService;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.context = context;

        if (!initialized) {
            this.topologyId = (String) conf.get(StormConfigKey.TOPOLOGY_ID);
            this.datasource = (String) conf.get(StormConfigKey.DATASOURCE);
            this.zkRoot = Utils.buildZKTopologyPath(topologyId);
            try {
                this.zkconnect = (String) conf.get(StormConfigKey.ZKCONNECT);
                /**
                 * 在此处创建zkservice
                 * 之后遇到APPENDER_TOPIC_RESUME时会重新生成
                 */
                zkService = new ZkService(zkconnect);

                PropertiesHolder.initialize(this.zkconnect, zkRoot);
                GlobalCache.initialize(this.datasource);

                PluginManagerProvider.initialize(new AppenderPluginLoader());

                //获取脱敏类型
                //分布式锁，初始化时，保证了集群中多个bolt只有一个去扫描classpath，
                //没有获取到锁的bolt线程不去扫描classpath
                //ExternalEncoders externalEncoders = new ExternalEncoders();
                //externalEncoders.initExternalEncoders(zkconnect, zkService);

                if (producer != null) {
                    producer.close();
                }
                producer = createProducer();

                handlerManager = new BoltHandlerManager(buildProvider());

                initialized = true;
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                throw new InitializationException(e);
            }
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
            logger.error("Process data error", e);
            this.collector.reportError(e);
            this.collector.fail(input);
        }

    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(EmitFields.GROUP_FIELD, EmitFields.DATA, EmitFields.COMMAND, EmitFields.EMIT_TYPE));
    }

    /**
     * 用于获取zkservices
     *
     * @return zkservice
     */

    @Override
    public ZkService getZkService() {
        return this.zkService;
    }

    /**
     * 用于重新加载zkservice
     * 这样就可以重新获取去UMS_UID
     */

    @Override
    public void reloadZkService() {
        if (zkService != null) {
            try {
                zkService.close();
            } catch (IOException e) {
                logger.error("zkservice close failed in WrapperBolt");
                e.printStackTrace();
            }
        }

        try {
            zkService = new ZkService(zkconnect);
            logger.info("zkservice reload success, new ums_uid get");
        } catch (Exception e) {
            logger.error("zkservice reload failed in WrapperBolt");
            e.printStackTrace();
        }
    }

    @Override
    public String getZkconnect() {
        return this.zkconnect;
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
            ThreadLocalCache.reload();
            PluginManagerProvider.reloadManager();
            if (producer != null) {
                producer.close();
            }

            producer = createProducer();

            if (zkService != null) {
                zkService.close();
            }

            zkService = new ZkService(zkconnect);

            //ExternalEncoders externalEncoders = new ExternalEncoders();
            //externalEncoders.initExternalEncoders(zkconnect, zkService);

            msg = "Wrapper write bolt reload successful!";
            logger.info("Wrapper bolt was reloaded at:{}", System.currentTimeMillis());
        } catch (Exception e) {
            msg = e.getMessage();
            throw new RuntimeException(e);
        } finally {
            if (tuple != null) {
                EmitData data = (EmitData) tuple.getValueByField(EmitFields.DATA);
                ControlMessage message = data.get(EmitData.MESSAGE);
                CtlMessageResult result = new CtlMessageResult("wrapper-bolt", msg);
                result.setOriginalMessage(message);
                CtlMessageResultSender sender = new CtlMessageResultSender(message.getType(), zkconnect);
                sender.send("wrapper-bolt-" + context.getThisTaskId(), result, false, true);
            }
        }
    }

    private Producer<String, String> createProducer() throws Exception {
        Properties props = PropertiesHolder.getProperties(Constants.Properties.PRODUCER_CONFIG);
        props.setProperty("client.id", this.topologyId + "_wrapper_" + context.getThisTaskId());

        Producer<String, String> producer = new KafkaProducer<>(props);
        return producer;
    }

    private BoltCommandHandlerProvider buildProvider() throws Exception {
        /*Set<Class<?>> classes = AnnotationScanner.scan("com.creditease.dbus", BoltCmdHandlerProvider.class, (clazz, annotation) -> {
            BoltCmdHandlerProvider ca = clazz.getAnnotation(BoltCmdHandlerProvider.class);
            return ca.type() == BoltType.WRAPPER_BOLT;
        });
        if (classes.size() <= 0) throw new ProviderNotFoundException();
        if (classes.size() > 1) {
            String providers = "";
            for (Class<?> clazz : classes) {
                providers += clazz.getName() + " ";
            }
            throw new RuntimeException("too many providers found: " + providers);
        }*/
        DbusDatasourceType type = GlobalCache.getDatasourceType();
        String name;
        if (type == DbusDatasourceType.ORACLE) {
            name = "com.creditease.dbus.stream.oracle.appender.bolt.processor.provider.WrapperCmdHandlerProvider";
        } else if (type == DbusDatasourceType.MYSQL) {
            name = "com.creditease.dbus.stream.mysql.appender.bolt.processor.provider.WrapperCmdHandlerProvider";
        }
        else {
            throw new IllegalArgumentException("Illegal argument [" + type.toString() + "] for building BoltCommandHandler map!");
        }
        Class<?> clazz = Class.forName(name);
        Constructor<?> constructor = clazz.getConstructor(CommandHandlerListener.class);
        return (BoltCommandHandlerProvider) constructor.newInstance(this);
    }
}
