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

import com.creditease.dbus.bolt.DataShardsSplittingBolt;
import com.creditease.dbus.bolt.PagedBatchDataFetchingBolt;
import com.creditease.dbus.bolt.ProgressBolt;
import com.creditease.dbus.common.FullPullConstants;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.helper.FullPullPropertiesHolder;
import com.creditease.dbus.spout.DataPullingSpout;
import com.creditease.dbus.spout.DataShardsSplittingSpout;
import org.apache.commons.cli.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Properties;


public class FullPullerTopology {
    private static String CURRENT_JAR_INFO = "dbus-fullpuller-" + Constants.RELEASE_VERSION + ".jar";
    private static String zkConnect;
    private static String fullSplitterTopologyId;
    private static String fullPullerTopologyId;
    private static String topologyId;
    private static String topologyName;
    private static boolean runAsLocal;
    private static String type;
    private static int splittingBoltParallel;
    private static int pullingBoltParallel;
    private static Properties commonConfSplit;
    private static Properties commonConfPull;

    public static void main(String[] args) throws Exception {
        int result = parseCommandArgs(args);
        if (result != 0) {
            return;
        }
        FullPullPropertiesHolder.initialize(FullPullConstants.FULL_SPLITTER_TYPE, zkConnect, FullPullConstants.TOPOLOGY_ROOT + "/" + FullPullConstants.FULL_SPLITTING_PROPS_ROOT);
        FullPullPropertiesHolder.initialize(FullPullConstants.FULL_PULLER_TYPE, zkConnect, FullPullConstants.TOPOLOGY_ROOT + "/" + FullPullConstants.FULL_PULLING_PROPS_ROOT);
        commonConfSplit = FullPullPropertiesHolder.getCommonConf(FullPullConstants.FULL_SPLITTER_TYPE, fullSplitterTopologyId);
        commonConfPull = FullPullPropertiesHolder.getCommonConf(FullPullConstants.FULL_PULLER_TYPE, fullPullerTopologyId);

        splittingBoltParallel = Integer.valueOf(commonConfSplit.getProperty(FullPullConstants.SPLITTING_BOLT_PARALLEL));
        pullingBoltParallel = Integer.valueOf(commonConfPull.getProperty(FullPullConstants.PULLING_BOLT_PARALLEL));

        //生成topology
        FullPullerTopology topology = new FullPullerTopology();
        StormTopology topo = topology.buildTopology(type);
        topology.start(topo, runAsLocal);
    }

    private static int parseCommandArgs(String[] args) {
        Options options = new Options();

        options.addOption("zk", "zookeeper", true, "the zookeeper address for properties files.");
        options.addOption("tid", "topology_id", true, "the unique id as topology name and root node name in zookeeper.");
        options.addOption("t", "type", true, "start splitter or pullter");
        options.addOption("l", "local", false, "run as local topology.");
        options.addOption("h", "help", false, "print usage().");

        CommandLineParser parser = new DefaultParser();
        try {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);

            if (line.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp(CURRENT_JAR_INFO, options);
                return -1;
            } else {
                runAsLocal = line.hasOption("local");
                zkConnect = line.getOptionValue("zookeeper");
                topologyId = line.getOptionValue("topology_id");
                if (line.hasOption("type")) {
                    type = line.getOptionValue("type");
                }

                if ("splitter".equals(type)) {
                    if ("global".equals(topologyId)) {
                        fullSplitterTopologyId = topologyId + "-fulldata-splitter";
                    } else {
                        fullSplitterTopologyId = topologyId + "-fullsplitter";
                    }
                    topologyName = topologyId + "-splitter";
                } else if ("puller".equals(type)) {
                    if ("global".equals(topologyId)) {
                        fullPullerTopologyId = topologyId + "-fulldata-puller";
                    } else {
                        fullPullerTopologyId = topologyId + "-fullpuller";
                    }
                    topologyName = topologyId + "-puller";
                } else {
                    if ("global".equals(topologyId)) {
                        fullSplitterTopologyId = topologyId + "-fulldata-splitter";
                        fullPullerTopologyId = topologyId + "-fulldata-puller";
                    } else {
                        fullSplitterTopologyId = topologyId + "-fullsplitter";
                        fullPullerTopologyId = topologyId + "-fullpuller";
                    }
                    topologyName = topologyId + "-splitter-puller";
                }

                if (zkConnect == null || fullSplitterTopologyId == null || fullPullerTopologyId == null) {
                    HelpFormatter formatter = new HelpFormatter();
                    formatter.printHelp(CURRENT_JAR_INFO, options);
                    return -1;
                }
            }
            return 0;
        } catch (ParseException exp) {
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
            return -2;
        }
    }

    private StormTopology buildTopology(String type) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        if ("splitter".equals(type)) {
            builder.setSpout("SplitSpout", new DataShardsSplittingSpout());
            builder.setBolt("SplitBolt", new DataShardsSplittingBolt(), splittingBoltParallel)
                    .allGrouping("SplitSpout", FullPullConstants.CTRL_STREAM)
                    .shuffleGrouping("SplitSpout");

        } else if ("puller".equals(type)) {
            builder.setSpout("PullSpout", new DataPullingSpout());
            builder.setBolt("PullBatchDataFetchBolt", new PagedBatchDataFetchingBolt(), pullingBoltParallel)
                    .shuffleGrouping("PullSpout");
            builder.setBolt("PullProcessBolt", new ProgressBolt())
                    .shuffleGrouping("PullBatchDataFetchBolt");
        } else {
            builder.setSpout("SplitSpout", new DataShardsSplittingSpout());
            builder.setBolt("SplitBolt", new DataShardsSplittingBolt(), splittingBoltParallel)
                    .allGrouping("SplitSpout", FullPullConstants.CTRL_STREAM)
                    .shuffleGrouping("SplitSpout");

            builder.setSpout("PullSpout", new DataPullingSpout());
            builder.setBolt("PullBatchDataFetchBolt", new PagedBatchDataFetchingBolt(), pullingBoltParallel)
                    .allGrouping("PullSpout", FullPullConstants.CTRL_STREAM)
                    .shuffleGrouping("PullSpout");

            builder.setBolt("PullProcessBolt", new ProgressBolt())
                    .allGrouping("PullBatchDataFetchBolt", FullPullConstants.CTRL_STREAM)
                    .shuffleGrouping("PullBatchDataFetchBolt");
        }
        return builder.createTopology();
    }


    private void start(StormTopology topology, boolean runAsLocal) throws Exception {
        Config conf = new Config();
        conf.put(FullPullConstants.FULL_SPLITTER_TOPOLOGY_ID, fullSplitterTopologyId);
        conf.put(FullPullConstants.FULL_PULLER_TOPOLOGY_ID, fullPullerTopologyId);
        conf.put(FullPullConstants.DS_NAME, topologyId);
        conf.put(FullPullConstants.ZKCONNECT, zkConnect);
        conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, commonConfSplit.getProperty(FullPullConstants.TOPOLOGY_WORKER_CHILDOPTS));
        //设置message超时时间为，保证每个分片都能在该内拉完数据
        conf.setMessageTimeoutSecs(Integer.parseInt(commonConfSplit.getProperty(FullPullConstants.STORM_MESSAGE_TIMEOUT)));
        conf.setMaxSpoutPending(Integer.parseInt(commonConfSplit.getProperty(FullPullConstants.STORM_MAX_SPOUT_PENDING)));
        conf.setNumWorkers(Integer.parseInt(commonConfSplit.getProperty(FullPullConstants.STORM_NUM_WORKERS)));
        conf.setDebug(true);

        if (runAsLocal) {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, conf, topology);
        } else {
            StormSubmitter.submitTopology(topologyName, conf, topology);
        }
    }
}
