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


package com.creditease.dbus.log.processor;

import com.creditease.dbus.log.processor.bolt.LogProcessorHeartbeatBolt;
import com.creditease.dbus.log.processor.bolt.LogProcessorKafkaWriteBolt;
import com.creditease.dbus.log.processor.bolt.LogProcessorStatBolt;
import com.creditease.dbus.log.processor.bolt.LogProcessorTransformBolt;
import com.creditease.dbus.log.processor.helper.ZkHelper;
import com.creditease.dbus.log.processor.spout.LogProcessorKafkaReadSpout;
import com.creditease.dbus.log.processor.util.Constants;
import org.apache.commons.cli.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Properties;

import static com.creditease.dbus.commons.Constants.LOG_PROCESSOR;

public class DBusLogProcessorTopology {
    private static String zkConnect;
    private static String topologyId;
    private static String topologyName;
    private static boolean runAsLocal;
    private static Properties properties;

    public static void main(String[] args) throws Exception {

        if (parseCommandArgs(args) != 0) {
            return;
        }
        ZkHelper zkHelper = new ZkHelper(zkConnect, topologyId);
        properties = zkHelper.loadLogProcessorConf();
        DBusLogProcessorTopology topology = new DBusLogProcessorTopology();
        topology.start(new DBusLogProcessorTopology().buildTopology(), runAsLocal);

    }

    private StormTopology buildTopology() throws Exception {
        int kafkaReadSpoutParallel = Integer.valueOf(properties.getProperty(Constants.LOG_KAFKA_READ_SPOUT_PARALLEL));
        int transformBoltParallel = Integer.valueOf(properties.getProperty(Constants.LOG_TRANSFORM_BOLT_PARALLEL));
        int kafkaWriteBoltParallel = Integer.valueOf(properties.getProperty(Constants.LOG_KAFKA_WRITE_BOLT_PARALLEL));
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("LogProcessorKafkaReadSpout",
                new LogProcessorKafkaReadSpout(),
                kafkaReadSpoutParallel);

        builder.setBolt("LogProcessorTransformBolt",
                new LogProcessorTransformBolt(), transformBoltParallel)
                .shuffleGrouping("LogProcessorKafkaReadSpout", "umsStream")
                .allGrouping("LogProcessorKafkaReadSpout", "ctrlOrHbStream");

        builder.setBolt("LogProcessorStatBolt",
                new LogProcessorStatBolt(), 1).
                shuffleGrouping("LogProcessorTransformBolt", "statStream").
                allGrouping("LogProcessorTransformBolt", "ctrlStream");

        builder.setBolt("LogProcessorHeartbeatBolt",
                new LogProcessorHeartbeatBolt(), 1).
                shuffleGrouping("LogProcessorTransformBolt", "umsStream").
                shuffleGrouping("LogProcessorTransformBolt", "heartbeatStream").
                allGrouping("LogProcessorTransformBolt", "ctrlStream");

        builder.setBolt("LogProcessorKafkaWriteBolt",
                new LogProcessorKafkaWriteBolt(),
                kafkaWriteBoltParallel).
                allGrouping("LogProcessorTransformBolt", "ctrlStream").
                shuffleGrouping("LogProcessorHeartbeatBolt", "heartbeatStream").
                shuffleGrouping("LogProcessorHeartbeatBolt", "umsStream").
                shuffleGrouping("LogProcessorStatBolt");

        return builder.createTopology();
    }

    private static int parseCommandArgs(String[] args) {
        Options options = new Options();

        options.addOption("zk", "zookeeper", true, "the zookeeper address for properties files.");
        options.addOption("tid", "topology_id", true, "the unique id as topology name and root node name in zookeeper.");
        options.addOption("l", "local", false, "run as local topology.");
        options.addOption("h", "help", false, "print usage().");

        CommandLineParser parser = new DefaultParser();
        try {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);
            runAsLocal = line.hasOption("local");
            zkConnect = line.getOptionValue("zookeeper");
            topologyId = line.getOptionValue("topology_id");
            topologyName = topologyId + "-" + LOG_PROCESSOR;
            return 0;
        } catch (ParseException exp) {
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
            return -2;
        }
    }

    private void start(StormTopology topology, boolean runAsLocal) throws Exception {
        Config conf = new Config();
        conf.put(com.creditease.dbus.commons.Constants.ZOOKEEPER_SERVERS, zkConnect);
        conf.put(Constants.TOPOLOGY_ID, topologyId);
        conf.setMessageTimeoutSecs(Integer.parseInt(properties.getProperty(Constants.LOG_MESSAGE_TIMEOUT)));
        //conf.setMaxSpoutPending(30);
        conf.setDebug(true);
        conf.setNumWorkers(Integer.parseInt(properties.getProperty(Constants.LOG_NUMWORKERS)));

        if (runAsLocal) {
            conf.setMaxTaskParallelism(10);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, conf, topology);
        } else {
            StormSubmitter.submitTopology(topologyName, conf, topology);
        }
    }


}
