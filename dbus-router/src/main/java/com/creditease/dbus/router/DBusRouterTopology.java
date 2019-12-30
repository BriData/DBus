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


package com.creditease.dbus.router;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.router.bolt.DBusRouterEncodeBolt;
import com.creditease.dbus.router.bolt.DBusRouterKafkaWriteBolt;
import com.creditease.dbus.router.bolt.DBusRouterStatBolt;
import com.creditease.dbus.router.facade.ZKFacade;
import com.creditease.dbus.router.spout.DBusRouterKafkaReadSpout;
import com.creditease.dbus.router.spout.DBusRouterMonitorSpout;
import com.creditease.dbus.router.util.DBusRouterConstants;
import org.apache.commons.cli.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Properties;

/**
 * Created by mal on 2018/5/17.
 */
public class DBusRouterTopology {

    private static String CURRENT_JAR_INFO = "dbus-router-" + Constants.RELEASE_VERSION + ".jar";
    private static String zkConnect;
    private static String topologyId;
    private static String alias;
    private static String projectName;
    private static String topologyName;
    private static boolean runAsLocal;
    private Properties routerConf;

    public static void main(String[] args) throws Exception {
        if (parseCommandArgs(args) != 0)
            return;
        DBusRouterTopology topology = new DBusRouterTopology();
        topology.start(topology.buildTopology(), runAsLocal);
    }

    private void loadRouterConf() throws Exception {
        ZKFacade zkHelper = new ZKFacade(zkConnect, topologyId, projectName);
        try {
            routerConf = zkHelper.loadRouterConf();
        } finally {
            zkHelper.close();
        }
    }

    private StormTopology buildTopology() throws Exception {
        // 加载router zk配置信息
        loadRouterConf();

        TopologyBuilder builder = new TopologyBuilder();

        int kafkaReadSpoutParallel =
                Integer.valueOf(routerConf.getProperty(DBusRouterConstants.STORM_KAFKA_READ_SPOUT_PARALLEL, "1"));
        builder.setSpout("RouterKafkaReadSpout",
                new DBusRouterKafkaReadSpout(),
                kafkaReadSpoutParallel);

        builder.setSpout("RouterMonitorSpout",
                new DBusRouterMonitorSpout(),
                1);

        int encodeBoltParallel =
                Integer.valueOf(routerConf.getProperty(DBusRouterConstants.STORM_ENCODE_BOLT_PARALLEL, "5"));
        builder.setBolt("RouterEncodeBolt",
                new DBusRouterEncodeBolt(), encodeBoltParallel)
                .fieldsGrouping("RouterKafkaReadSpout", "umsOrHbStream", new Fields("ns"))
                .allGrouping("RouterKafkaReadSpout", "ctrlStream");

        int statBoltParallel = 1;
        builder.setBolt("RouterStatBolt",
                new DBusRouterStatBolt(), statBoltParallel)
                .shuffleGrouping("RouterEncodeBolt", "statStream")
                .shuffleGrouping("RouterEncodeBolt", "ctrlStream");

        int kafkaWriteBoltParallel =
                Integer.valueOf(routerConf.getProperty(DBusRouterConstants.STORM_KAFKA_WREIT_BOLT_PARALLEL, "1"));
        builder.setBolt("RouterKafkaWriteBolt",
                new DBusRouterKafkaWriteBolt(),
                kafkaWriteBoltParallel).
                allGrouping("RouterEncodeBolt", "ctrlStream").
                shuffleGrouping("RouterEncodeBolt", "umsOrHbStream").
                shuffleGrouping("RouterStatBolt", "statStream");

        return builder.createTopology();
    }

    private void start(StormTopology topology, boolean runAsLocal) throws Exception {
        Config conf = new Config();
        conf.put(com.creditease.dbus.commons.Constants.ZOOKEEPER_SERVERS, zkConnect);
        conf.put(Constants.TOPOLOGY_ID, topologyId);
        conf.put(Constants.TOPOLOGY_ALIAS, alias);
        conf.put(Constants.ROUTER_PROJECT_NAME, projectName);

        String workerChildOpts = routerConf.getProperty(DBusRouterConstants.STORM_TOPOLOGY_WORKER_CHILDOPTS, "-Xmx2g");
        conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, workerChildOpts);

        int msgTimeout = Integer.valueOf(routerConf.getProperty(DBusRouterConstants.STORM_MESSAGE_TIMEOUT, "10"));
        conf.setMessageTimeoutSecs(msgTimeout);

        int maxSpoutPending = Integer.valueOf(routerConf.getProperty(DBusRouterConstants.STORM_MAX_SPOUT_PENDING, "100"));
        conf.setMaxSpoutPending(maxSpoutPending);
        conf.setDebug(true);

        int numWorks = Integer.valueOf(routerConf.getProperty(DBusRouterConstants.STORM_NUM_WORKS, "1"));
        conf.setNumWorkers(numWorks);

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
        options.addOption("alias", "alias", true, "the unique id as topology alias and as part of ums namespace.");
        options.addOption("pn", "project_name", true, "as part of the configuration path in zookeeper.");
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
                topologyId = line.getOptionValue("topology_id");
                alias = line.getOptionValue("alias");
                projectName = line.getOptionValue("project_name");
                topologyName = topologyId + "-" + Constants.ROUTER;
                if (zkConnect == null || topologyId == null || alias == null | projectName == null) {
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
