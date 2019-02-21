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

package com.creditease.dbus.stream;

import com.creditease.dbus.commons.PropertiesHolder;
import com.creditease.dbus.stream.appender.bolt.*;
import com.creditease.dbus.stream.appender.spout.DbusKafkaSpout;
import com.creditease.dbus.stream.appender.utils.DbusGrouping;
import com.creditease.dbus.stream.common.Constants;

import com.creditease.dbus.stream.dispatcher.Spout.KafkaConsumerSpout;

import com.creditease.dbus.stream.dispatcher.bout.DispatcherBout;
import com.creditease.dbus.stream.dispatcher.bout.KafkaProducerBout;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DispatcherAppenderTopology {

    /**
     * Appender部分
     */

    private static Logger logger = LoggerFactory.getLogger(DispatcherAppenderTopology.class);
    private static String zookeeper;
    private static String topologyId;
    private static String dispatcherTopologyId;
    private static String appenderTopologyId;
    private static String topologyType;
    private static boolean runAsLocal;
    private String datasource;
    private String datasourceType;

    public static void main(String[] args) throws Exception {

        int result = parseCommandArgs(args);
        if (result != 0) {
            return;
        }

        DispatcherAppenderTopology topology = new DispatcherAppenderTopology();


        StormTopology top = topology.buildTopology();
        topology.start(top, runAsLocal);
    }

    private static int parseCommandArgs(String[] args) {
        Options options = new Options();

        options.addOption("zk", "zookeeper", true, "the zookeeper address for properties files.");
        options.addOption("tid", "topology_id", true, "the unique id as topology name and root node name in zookeeper.");
        options.addOption("t", "type", true, "the topology you want to start, it can be dispatcher, appender or all. If not exist, both dispatcher and appender will start.");
        options.addOption("l", "local", false, "run as local topology.");
        options.addOption("h", "help", false, "print usage().");

        CommandLineParser parser = new DefaultParser();
        try {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);

            if (line.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("dbus-ora-appender-1.0.jar", options);

                return -1;
            } else {

                runAsLocal = line.hasOption("local");
                zookeeper = line.getOptionValue("zookeeper");
                if (line.hasOption("type")) {
                    topologyType = line.getOptionValue("type");
                } else {
                    topologyType = Constants.TopologyType.ALL;
                }
                /**
                 * 这里topologyIdPrefix仅仅是一个前缀
                 * 比如mydb，cedb等
                 */
                String topologyIdPrefix = line.getOptionValue("topology_id");

                if (zookeeper == null || topologyIdPrefix == null) {
                    HelpFormatter formatter = new HelpFormatter();
                    formatter.printHelp("dbus-ora-appender-1.0.jar", options);
                    return -1;
                }

                if (!topologyType.equals(Constants.TopologyType.ALL)
                        && !topologyType.equals(Constants.TopologyType.DISPATCHER)
                        && !topologyType.equals(Constants.TopologyType.APPENDER)) {
                    HelpFormatter formatter = new HelpFormatter();
                    formatter.printHelp("dbus-ora-appender-1.0.jar", options);
                    return -1;
                }

                dispatcherTopologyId = StringUtils.join(new String[]{topologyIdPrefix, Constants.TopologyType.DISPATCHER}, "-");
                appenderTopologyId = StringUtils.join(new String[]{topologyIdPrefix, Constants.TopologyType.APPENDER}, "-");

                if (topologyType.equals(Constants.TopologyType.ALL)) {
                    topologyId = StringUtils.join(new String[]{topologyIdPrefix, Constants.TopologyType.DISPATCHER, Constants.TopologyType.APPENDER}, "-");
                } else {
                    topologyId = StringUtils.join(new String[]{topologyIdPrefix, topologyType}, "-");
                }
            }
            return 0;
        } catch (ParseException exp) {
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
            return -2;
        }
    }

    private void initializeDispatcher(String zkconnect, String root) throws Exception {
        // 初始化配置文件
        PropertiesHolder.initialize(zkconnect, root);
        String dispatcher_configure = com.creditease.dbus.commons.Constants.DISPATCHER_CONFIG_PROPERTIES;
        this.datasourceType = PropertiesHolder.getProperties(dispatcher_configure, com.creditease.dbus.commons.Constants.DBUS_DATASOURCE_TYPE);
    }

    private void initializeAppender(String zkconnect, String root) throws Exception {
        // 初始化配置文件
        PropertiesHolder.initialize(zkconnect, root);
        String configure = Constants.Properties.CONFIGURE;

        this.datasource = PropertiesHolder.getProperties(configure, Constants.ConfigureKey.DATASOURCE_NAME);
    }

    private StormTopology buildTopology() throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        // 启动类型为all，或者dispatcher
        if (topologyType.equals(Constants.TopologyType.ALL) || topologyType.equals(Constants.TopologyType.DISPATCHER)) {
            /**
             * dispatcher部分
             */
            //dispatcher部分
            this.initializeDispatcher(zookeeper, Constants.ZKPath.ZK_TOPOLOGY_ROOT + "/" + dispatcherTopologyId);

                builder.setSpout("dispatcher-kafkaConsumerSpout", new KafkaConsumerSpout(), 1);
                builder.setBolt("dispatcher-DispatcherBout", new DispatcherBout(), 1)
                        .shuffleGrouping("dispatcher-kafkaConsumerSpout");
                builder.setBolt("dispatcher-kafkaProducerBout", new KafkaProducerBout(), 1)
                        .shuffleGrouping("dispatcher-DispatcherBout");
        }

        // 启动类型为all，或者appender
        if (topologyType.equals(Constants.TopologyType.ALL) || topologyType.equals(Constants.TopologyType.APPENDER)) {
            /**
             * appender部分
             */
            // 初始化配置文件
            this.initializeAppender(zookeeper, Constants.ZKPath.ZK_TOPOLOGY_ROOT + "/" + appenderTopologyId);
            builder.setSpout("appender-spout", new DbusKafkaSpout(), 1);
            builder.setBolt("appender-dispatcher", new DispatcherBolt(), 1).shuffleGrouping("appender-spout");
            builder.setBolt("appender-meta-fetcher", new DbusAppenderBolt(), getBoltParallelism(Constants.ConfigureKey.META_FETCHER_BOLT_PARALLELISM, 3))
                    .customGrouping("appender-dispatcher", new DbusGrouping());
            builder.setBolt("appender-wrapper", new WrapperBolt(), getBoltParallelism(Constants.ConfigureKey.WRAPPER_BOLT_PARALLELISM, 3))
                    .customGrouping("appender-meta-fetcher", new DbusGrouping());
            builder.setBolt("appender-kafka-writer", new DbusKafkaWriterBolt(), getBoltParallelism(Constants.ConfigureKey.KAFKA_WRITTER_BOLT_PARALLELISM, 3))
                    .customGrouping("appender-wrapper", new DbusGrouping());

            // 为了避免dbus-router统计信息与dispatcher和appender不一致的情况，将写心跳到kafka中的逻辑提前到appender-kafka-writer中
            //builder.setBolt("appender-heart-beat", new DbusHeartBeatBolt(), 1).shuffleGrouping("appender-kafka-writer");
        }


        return builder.createTopology();
    }

    private int getBoltParallelism(String key, int defaultValue) {
        return getConfigureValueWithDefault(key, defaultValue);
    }

    private int getConfigureValueWithDefault(String key, int defaultValue) {
        Integer num = PropertiesHolder.getIntegerValue(Constants.Properties.CONFIGURE, key);
        return num == null ? defaultValue: num.intValue();
    }
    private void start(StormTopology topology, boolean runAsLocal) throws Exception {

        Config conf = new Config();

        // 启动类型为all，或者dispatcher
        if (topologyType.equals(Constants.TopologyType.ALL) || topologyType.equals(Constants.TopologyType.DISPATCHER)) {
            /**
             * dispatcher配置
             */

            conf.put(com.creditease.dbus.commons.Constants.ZOOKEEPER_SERVERS, zookeeper);
            conf.put(com.creditease.dbus.commons.Constants.TOPOLOGY_ID, dispatcherTopologyId);
            logger.info(com.creditease.dbus.commons.Constants.ZOOKEEPER_SERVERS + "=" + zookeeper);
            logger.info(com.creditease.dbus.commons.Constants.TOPOLOGY_ID + "=" + dispatcherTopologyId);
        }

        // 启动类型为all，或者appender
        if (topologyType.equals(Constants.TopologyType.ALL) || topologyType.equals(Constants.TopologyType.APPENDER)) {

            /**
             * appender配置
             */
            conf.put(Constants.StormConfigKey.TOPOLOGY_ID, appenderTopologyId);
            conf.put(Constants.StormConfigKey.ZKCONNECT, zookeeper);
            conf.put(Constants.StormConfigKey.DATASOURCE, datasource);
        }

//        conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 4096);
//        conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 4096);
//        conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 4096);

        conf.setDebug(true);

        conf.setNumAckers(1);
        //设置worker数
        conf.setNumWorkers(1);
        //设置任务在发出后，但还没处理完成的中间状态任务的最大数量, 如果没有设置最大值为50
        int MaxSpoutPending = getConfigureValueWithDefault(Constants.ConfigureKey.MAX_SPOUT_PENDING, 50);
        conf.setMaxSpoutPending(MaxSpoutPending);
        //设置任务在多久之内没处理完成，则这个任务处理失败
        conf.setMessageTimeoutSecs(120);

//        conf.put(Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS, true);
//        conf.registerSerialization(org.apache.avro.util.Utf8.class);
//        conf.registerSerialization(com.creditease.dbus.commons.DBusConsumerRecord.class);
//        conf.registerSerialization(org.apache.kafka.common.record.TimestampType.class);
//        conf.registerSerialization(com.creditease.dbus.stream.common.appender.bean.EmitData.class);
//        conf.registerSerialization(com.creditease.dbus.stream.common.appender.enums.Command.class);
//        conf.registerSerialization(org.apache.avro.generic.GenericData.class);
//        conf.registerSerialization(com.creditease.dbus.stream.oracle.appender.avro.GenericData.class);
//        conf.registerSerialization(com.creditease.dbus.commons.DbusMessage12.class);
//        conf.registerSerialization(com.creditease.dbus.commons.DbusMessage12.Schema12.class);
//        conf.registerSerialization(com.creditease.dbus.commons.DbusMessage13.Schema13.class);
//        conf.registerSerialization(com.creditease.dbus.commons.DbusMessage13.class);
//        conf.registerSerialization(com.creditease.dbus.commons.DbusMessage.Field.class);
//        conf.registerSerialization(com.creditease.dbus.commons.DbusMessage.Payload.class);
//        conf.registerSerialization(com.creditease.dbus.commons.DbusMessage.Protocol.class);
//        conf.registerSerialization(com.creditease.dbus.commons.DbusMessage.ProtocolType.class);
//        conf.registerSerialization(com.creditease.dbus.stream.oracle.appender.bolt.processor.appender.OraWrapperData.class);
//        conf.registerSerialization(com.creditease.dbus.stream.common.appender.spout.cmds.TopicResumeCmd.class);

        if (runAsLocal) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyId, conf, topology);
            /*String cmd;
            do {
                cmd = System.console().readLine();
            } while (!cmd.equals("exit"));
            cluster.shutdown();*/
        } else {
            StormSubmitter.submitTopology(topologyId, conf, topology);
        }
    }
}
