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


package com.creditease.dbus.stream.dispatcher.bout;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.ControlType;
import com.creditease.dbus.commons.DBusConsumerRecord;
import com.creditease.dbus.stream.common.DataSourceInfo;
import com.creditease.dbus.stream.common.bean.DispatcherPackage;
import com.creditease.dbus.stream.common.tools.MessageProcessor;
import com.creditease.dbus.stream.common.tools.TableStatMap;
import com.creditease.dbus.stream.dispatcher.helper.DBHelper;
import com.creditease.dbus.stream.dispatcher.helper.ZKHelper;
import com.creditease.dbus.stream.dispatcher.tools.ContinuousFullyOffset;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;

public class Db2DispatcherBout extends BaseRichBolt {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    private Properties schemaTopicProps = null;

    private String zkServers = null;
    private String topologyID = null;
    private String topologyRoot = null;

    private DataSourceInfo dsInfo = null;
    private String statTopic = null;


    private MessageProcessor processor = null;
    protected TableStatMap statMap = new TableStatMap();

    //按照schema来保存的上一次的global fully offset
    private Map<String, ContinuousFullyOffset> schemaOffsetMap = new HashMap<>();

    private Map conf = null;
    private OutputCollector collector = null;


    /**
     * getSchemaFullyOffset
     *
     * @param topic
     * @return
     */
    private ContinuousFullyOffset getSchemaFullyOffset(String topic) {

        ContinuousFullyOffset fullyOffset = schemaOffsetMap.get(topic);
        if (fullyOffset == null) {
            fullyOffset = new ContinuousFullyOffset();
            schemaOffsetMap.put(topic, fullyOffset);
        }
        return fullyOffset;
    }

    /**
     * print log in logger
     *
     * @param props
     */
    private void printSchemaProps(Properties props) {
        logger.info(String.format("All schemas and topics as:"));

        Iterator it = props.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry entry = (Map.Entry) it.next();
            logger.info(String.format("%s=%s", entry.getKey(), entry.getValue()));
        }
    }

    /**
     * @param reloadJson: reload control msg json
     */
    private void reloadConfig(String reloadJson) {
        cleanup();

        ZKHelper zkHelper = null;
        DBHelper dbHelper = null;
        try {
            dsInfo = new DataSourceInfo();

            zkHelper = new ZKHelper(topologyRoot, topologyID, zkServers);
            zkHelper.loadDsNameAndOffset(dsInfo);
            logger.info(String.format("Bout read datasource: %s", dsInfo.toString()));

            //从zk中加载SchemaRegistryRestUrl
            dsInfo.setSchemaRegistryRestUrl(zkHelper.getSchemaRegistryRestUrl());

            //read info from mysql
            dbHelper = new DBHelper(zkServers);
            dbHelper.loadDsInfo(dsInfo);
            schemaTopicProps = dbHelper.getSchemaProps(dsInfo);
            printSchemaProps(schemaTopicProps);

            //save schemaTopics to zookeeper
            zkHelper.saveSchemaProps(schemaTopicProps);
            zkHelper.saveReloadStatus(reloadJson, "dispatcher-bout", false);

            //stat producer 的初始化
            Properties statProps = zkHelper.getStatProducerProps();
            statMap.initDsName(dsInfo.getDbSourceName());
            //KafkaProducer statProducer = new KafkaProducer<>(statProps);

            statTopic = zkHelper.getStatisticTopic();
            processor = getMessageProcessor(dsInfo, statTopic, statProps, statMap, schemaTopicProps);
        } catch (Exception ex) {
            logger.error("DispatcherBout reloadConfig():", ex);
            collector.reportError(ex);
            throw new RuntimeException(ex);
        } finally {
            if (dbHelper != null) {
                dbHelper.close();
            }
            if (zkHelper != null) {
                zkHelper.close();
            }
        }
    }

    public MessageProcessor getMessageProcessor(DataSourceInfo dsInfo, String statTopic, Properties statProps, TableStatMap statMap, Properties schemaTopicProps) throws Exception {
        if (dsInfo.getDbSourceType().equals("oracle")) {

            Class clazz = Class.forName("com.creditease.dbus.stream.oracle.dispatcher.OracleMessageProcessor");
            Constructor<?> constructor = clazz.getConstructor(DataSourceInfo.class, String.class, Properties.class, TableStatMap.class, Properties.class);
            return (MessageProcessor) constructor.newInstance(dsInfo, statTopic, statProps, statMap, schemaTopicProps);

        } else if (dsInfo.getDbSourceType().equals("mysql")) {

            Class clazz = Class.forName("com.creditease.dbus.stream.mysql.dispatcher.MysqlMessageProcessor");
            Constructor<?> constructor = clazz.getConstructor(DataSourceInfo.class, String.class, Properties.class, TableStatMap.class, Properties.class);
            return (MessageProcessor) constructor.newInstance(dsInfo, statTopic, statProps, statMap, schemaTopicProps);

        } else if (dsInfo.getDbSourceType().equals("mongo")) {

            Class clazz = Class.forName("com.creditease.dbus.stream.mongo.dispatcher.MongoMessageProcessor");
            Constructor<?> constructor = clazz.getConstructor(DataSourceInfo.class, String.class, Properties.class, TableStatMap.class, Properties.class);
            return (MessageProcessor) constructor.newInstance(dsInfo, statTopic, statProps, statMap, schemaTopicProps);

        } else if (dsInfo.getDbSourceType().equals("db2")) {

            Class clazz = Class.forName("com.creditease.dbus.stream.db2.dispatcher.Db2MessageProcessor");
            Constructor<?> constructor = clazz.getConstructor(DataSourceInfo.class, String.class, Properties.class, TableStatMap.class, Properties.class);
            return (MessageProcessor) constructor.newInstance(dsInfo, statTopic, statProps, statMap, schemaTopicProps);

        } else {
            throw new RuntimeException("Unknown datasource type" + dsInfo.getDbSourceType());
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.conf = stormConf;
        this.collector = collector;

        //only init one time
        zkServers = (String) stormConf.get(Constants.ZOOKEEPER_SERVERS);
        topologyID = (String) stormConf.get(Constants.TOPOLOGY_ID);
        topologyRoot = Constants.TOPOLOGY_ROOT + "/" + topologyID;

        reloadConfig(null);
        logger.info("DispatcherBolt reload config success !");
    }


    private void processControlCommand(DBusConsumerRecord<String, byte[]> record, Tuple input) {
        try {
            String key = record.key();
            String json = new String(record.value(), "utf-8");
            ControlType cmd = ControlType.getCommand(key);
            switch (cmd) {
                case DISPATCHER_RELOAD_CONFIG:
                    reloadConfig(json);
                    break;

                default:
                    /* do nothing */
                    break;
            }
            this.collector.ack(input);
        } catch (Exception ex) {
            logger.error("DispatcherBout processControlCommand():", ex);
            collector.reportError(ex);
            this.collector.fail(input);
        }

    }

    @Override
    public void execute(Tuple input) {

        String emitType = (String) input.getValueByField("emitType");
        List<DBusConsumerRecord<String, byte[]>> recordList = new ArrayList<>();
        if (StringUtils.equals("data", emitType)) {
            recordList = (List<DBusConsumerRecord<String, byte[]>>) input.getValueByField("record");
        } else {
            recordList.add((DBusConsumerRecord<String, byte[]>) input.getValueByField("record"));
        }

        try {
            //处理ctrl topic的数据
            if (recordList.size() == 1 && StringUtils.equals(recordList.get(0).topic(), dsInfo.getCtrlTopic())) {
                processControlCommand(recordList.get(0), input);
                return;
            }

            // a 读取message数据
            try {
                processor.db2PreProcess(recordList);
            } catch (Exception e) {
                e.printStackTrace();
            }

            // b 一次读取一个partition
            List<DispatcherPackage> list;

            do {
                try {
                    list = processor.getNextList();
                    if (list == null) {
                        break;
                    }
                    //分schema后的子包
                    for (DispatcherPackage subPackage : list) {
                        this.collector.emit(input, new Values(subPackage));
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

            } while (true);

            this.collector.ack(input);

        } catch (Exception ex) {
            logger.error("FAIL! Dispatcher bolt failed!" + ex);
            this.collector.fail(input);

            collector.reportError(ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void cleanup() {
        try {
            if (processor != null) {
                processor.cleanup();
                processor = null;
            }
        } catch (Exception ex) {
            // NOTE: Handle the failure
            logger.error("DispatcherBout cleanup():", ex);
            collector.reportError(ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("subPackage"));
    }
}
