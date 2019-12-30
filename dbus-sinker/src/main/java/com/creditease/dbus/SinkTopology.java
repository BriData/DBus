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


package com.creditease.dbus;

import com.creditease.dbus.bolt.SinkerWriteBolt;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.helper.ZKHepler;
import com.creditease.dbus.spout.SinkerKafkaReadSpout;
import com.creditease.dbus.tools.SinkerConstants;
import org.apache.commons.cli.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Properties;

public class SinkTopology {
    private static String CURRENT_JAR_INFO = "dbus-sinker-" + Constants.RELEASE_VERSION + ".jar";
    private static String zkConnect;
    private static String sinkType;
    private static String topologyId;
    private static String topologyName;
    private static boolean runAsLocal;
    private static Properties sinkerConf;

    public static void main(String[] args) throws Exception {
        if (parseCommandArgs(args) != 0)
            return;
        SinkTopology topology = new SinkTopology();
        topology.start(topology.buildTopology(), runAsLocal);
    }

    private void loadSinkerConf() throws Exception {
        ZKHepler zkHelper = new ZKHepler(zkConnect, topologyId);
        try {
            sinkerConf = zkHelper.loadSinkerConf(SinkerConstants.CONFIG);
        } finally {
            zkHelper.close();
        }
    }

    private StormTopology buildTopology() throws Exception {
        loadSinkerConf();

        Integer spoutSize = Integer.parseInt(sinkerConf.getProperty(SinkerConstants.STORM_KAFKA_READ_SPOUT_PARALLEL));
        Integer boltSize = Integer.parseInt(sinkerConf.getProperty(SinkerConstants.STORM_WRITE_BOUT_PARALLEL));
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("SinkerKafkaReadSpout", new SinkerKafkaReadSpout(), spoutSize);
        builder.setBolt("SinkerWriteBolt", new SinkerWriteBolt(), boltSize)
                .fieldsGrouping("SinkerKafkaReadSpout", "dataStream", new Fields("ns"))
                .allGrouping("SinkerKafkaReadSpout", "ctrlStream");

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(1);

        return builder.createTopology();
    }

    private void start(StormTopology topology, boolean runAsLocal) throws Exception {
        Config conf = new Config();
        conf.put(Constants.ZOOKEEPER_SERVERS, zkConnect);
        conf.put(Constants.TOPOLOGY_ID, topologyId);
        conf.put(Constants.SINK_TYPE, sinkType);
        conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, sinkerConf.getProperty(SinkerConstants.TOPOLOGY_WORKER_CHILDOPTS));

        conf.setMessageTimeoutSecs(Integer.parseInt(sinkerConf.getProperty(SinkerConstants.STORM_MESSAGE_TIMEOUT)));
        conf.setMaxSpoutPending(Integer.parseInt(sinkerConf.getProperty(SinkerConstants.STORM_MAX_SPOUT_PENDING)));
        conf.setDebug(true);
        conf.setNumWorkers(Integer.parseInt(sinkerConf.getProperty(SinkerConstants.STORM_NUM_WORKERS)));
        if (runAsLocal) {
            conf.setMaxTaskParallelism(10);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, conf, topology);
        } else {
            StormSubmitter.submitTopology(topologyName, conf, topology);
        }
    }

    private static int parseCommandArgs(String[] args) {
        Options options = new Options();
        options.addOption("zk", "zookeeper", true, "the zookeeper address for properties files.");
        options.addOption("tid", "topology_id", true, "the unique id as topology name and as part of the configuration path in zookeeper.");
        options.addOption("sty", "sink_type", true, "the sink type for topology.");
        options.addOption("l", "local", false, "run as local topology.");
        options.addOption("h", "help", false, "print usage().");
        CommandLineParser parser = new DefaultParser();
        int ret = 0;
        try {
            CommandLine line = parser.parse(options, args);
            if (line.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp(CURRENT_JAR_INFO, options);
                return -1;
            } else {
                runAsLocal = line.hasOption("local");
                zkConnect = line.getOptionValue("zookeeper");
                sinkType = line.getOptionValue("sink_type");
                topologyId = line.getOptionValue("topology_id");
                topologyName = topologyId + "-sinker";
                if (zkConnect == null || sinkType == null || topologyId == null) {
                    HelpFormatter formatter = new HelpFormatter();
                    formatter.printHelp(CURRENT_JAR_INFO, options);
                    return -1;
                }
            }
        } catch (ParseException exp) {
            System.err.println("Parsing failed. Reason: " + exp.getMessage());
            ret = -1;
        }
        return ret;
    }
}
