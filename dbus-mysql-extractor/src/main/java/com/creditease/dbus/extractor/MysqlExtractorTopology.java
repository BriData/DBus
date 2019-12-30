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


package com.creditease.dbus.extractor;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.extractor.bolt.KafkaProducerBolt;
import com.creditease.dbus.extractor.spout.CanalClientSpout;
import org.apache.commons.cli.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ximeiwang on 2017/8/16.
 */
public class MysqlExtractorTopology {
    //TODO
    private static Logger logger = LoggerFactory.getLogger(MysqlExtractorTopology.class);
    private static String CURRENT_JAR_INFO = "dbus-mysql-extractor-" + Constants.RELEASE_VERSION + ".jar";
    private static String zkServers;
    private static String topologyId;
    private static String extractorTopologyId;
    private static boolean runAsLocal;

    //解析命令行传参，获取zookeeper servers和topology ID
    private int parseCommandArgs(String[] args) {
        Options options = new Options();
        options.addOption("zk", "zookeeper", true, "the zookeeper address for properties files.");
        options.addOption("tid", "topology_id", true, "the unique id as topology name and root node name in zookeeper.");
        options.addOption("l", "local", false, "run as local topology.");
        options.addOption("h", "help", false, "print usage().");
        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine line = parser.parse(options, args);
            if (line.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp(CURRENT_JAR_INFO, options);
                return -1;
            } else {
                runAsLocal = line.hasOption("local");
                zkServers = line.getOptionValue("zookeeper");
                topologyId = line.getOptionValue("topology_id");
                extractorTopologyId = topologyId + "-mysql-extractor";
                if (zkServers == null || topologyId == null) {
                    HelpFormatter formatter = new HelpFormatter();
                    formatter.printHelp(CURRENT_JAR_INFO, options);
                    return -1;
                }
            }
            return 0;
        } catch (ParseException exp) {
            logger.error("Parsing failed.  Reason: " + exp.getMessage());
            exp.printStackTrace();
            return -2;
        }
    }

    public void buildTopology(String[] args) {
        //TODO
        if (parseCommandArgs(args) != 0) {
            return;
        }
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("CanalClientSpout", new CanalClientSpout(), 1);
        builder.setBolt("KafkaProducerBolt", new KafkaProducerBolt(), 1).shuffleGrouping("CanalClientSpout");

        Config conf = new Config();
        conf.put(Constants.ZOOKEEPER_SERVERS, zkServers);
        conf.put(Constants.EXTRACTOR_TOPOLOGY_ID, extractorTopologyId);
        logger.info(Constants.ZOOKEEPER_SERVERS + "=" + zkServers);
        logger.info(Constants.EXTRACTOR_TOPOLOGY_ID + "=" + extractorTopologyId);
        conf.setNumWorkers(1);
        conf.setMaxSpoutPending(50);
        conf.setMessageTimeoutSecs(120);
        if (!runAsLocal) {
            conf.setDebug(false);
            try {
                //StormSubmitter.submitTopology("extractorTopologyId", conf, builder.createTopology());
                StormSubmitter.submitTopology(extractorTopologyId, conf, builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            conf.setDebug(false);
            LocalCluster cluster = new LocalCluster();
            //cluster.submitTopology("extractorTopologyId", conf, builder.createTopology());
            cluster.submitTopology(extractorTopologyId, conf, builder.createTopology());
        }
    }

    public static void main(String[] args) {
        MysqlExtractorTopology top = new MysqlExtractorTopology();
        top.buildTopology(args);
        logger.info("extractor having started......");
    }
}
