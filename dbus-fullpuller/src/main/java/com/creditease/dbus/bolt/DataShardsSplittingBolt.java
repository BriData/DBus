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

package com.creditease.dbus.bolt;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.common.CommandCtrl;
import com.creditease.dbus.common.DBHelper;
import com.creditease.dbus.common.DataPullConstants;
import com.creditease.dbus.common.FullPullHelper;
import com.creditease.dbus.common.ProgressInfo;
import com.creditease.dbus.common.utils.DBConfiguration;
import com.creditease.dbus.common.utils.DataDrivenDBInputFormat;
import com.creditease.dbus.common.utils.InputSplit;
import com.creditease.dbus.common.utils.JsonUtil;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.Constants.ZkTopoConfForFullPull;
import com.creditease.dbus.commons.ZkService;
import com.creditease.dbus.commons.exception.InitializationException;
import com.creditease.dbus.manager.GenericJdbcManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class DataShardsSplittingBolt extends BaseRichBolt {
    private Logger LOG = LoggerFactory.getLogger(getClass());
    private static final long serialVersionUID = 1L;
    private boolean initialized = false;

    private String topologyId;
    //是否是独立拉全量
    private boolean isGlobal;
    private String zkMonitorRootNodePath;
    private String zkconnect;
    private String dsName;
    private String zkTopoRoot;
    ZkService zkService = null;

    private OutputCollector collector;
    private Properties commonProps;
    private Producer byteProducer;
    private Map confMap;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.topologyId = (String) conf.get(Constants.StormConfigKey.FULL_SPLITTER_TOPOLOGY_ID);
        this.isGlobal = this.topologyId.toLowerCase().indexOf(ZkTopoConfForFullPull.GLOBAL_FULLPULLER_TOPO_PREFIX) != -1 ? true : false;
        if (this.isGlobal) {
            this.zkMonitorRootNodePath = Constants.FULL_PULL_MONITOR_ROOT_GLOBAL;
        } else {
            this.zkMonitorRootNodePath = Constants.FULL_PULL_MONITOR_ROOT;
        }
        this.zkconnect = (String) conf.get(Constants.StormConfigKey.ZKCONNECT);
        this.zkTopoRoot = Constants.TOPOLOGY_ROOT + "/" + Constants.FULL_SPLITTING_PROPS_ROOT;
        if (!initialized) {
            // 初始化配置文件
            try {
                //设置topo类型，用于获取配置信息路径
                FullPullHelper.setTopologyType(Constants.FULL_SPLITTER_TYPE);

                loadRunningConf(null);
            } catch (Exception e) {
                throw new InitializationException(e);
            }
            initialized = true;
        }
    }

    @Override
    public void execute(Tuple input) {
        String data = (String) input.getValue(0);
        String dataType = JSONObject.parseObject(data).getString("type");
        if (dataType == null) {
            LOG.error("dataType is null in DataShardsSplittingBolt");
            return;
        }

        if (dataType.equals(DataPullConstants.COMMAND_FULL_PULL_RELOAD_CONF)) {
            try {
                //重新加载配置
                loadRunningConf(data);
                //将reload请求通过中间topic传导到puller
                String fullPullMediantTopic = commonProps.getProperty(Constants.ZkTopoConfForFullPull.FULL_PULL_MEDIANT_TOPIC);
                String dataSourceInfo = (String) input.getValue(0);
                JSONObject wrapperObject = new JSONObject();
                wrapperObject.put(DataPullConstants.DATA_SOURCE_INFO, dataSourceInfo);
                ProducerRecord record = new ProducerRecord<>(
                        fullPullMediantTopic,
                        DataPullConstants.COMMAND_FULL_PULL_RELOAD_CONF,
                        wrapperObject.toString().getBytes());
                byteProducer.send(record);
                LOG.info("Conf for SplittingBolt is reloaded successfully!");
            } catch (Exception e) {
                LOG.error("Reloading Conf for SplittingBolt is reloaded failed!");
            }
        } else if (dataType.equals(DataPullConstants.DATA_EVENT_FULL_PULL_REQ)
                || dataType.equals(DataPullConstants.DATA_EVENT_INDEPENDENT_FULL_PULL_REQ)) {
            doSplitting(input, dataType);
        } else if (dataType.equals(DataPullConstants.COMMAND_FULL_PULL_STOP)) {
            stopTopo(input);
        } else {
            LOG.error("Unknown dataType :{} in DataShardsSplittingBolt", dataType);
        }
    }

    /*
     * (non-Javadoc)
     * @see
     * backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.
     * topology.OutputFieldsDeclarer)
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public void doSplitting(Tuple input, String dataType) {
        String dataSourceInfo = (String) input.getValue(0);
        String errorMsg = "";

        boolean metaCompatible = FullPullHelper.validateMetaCompatible(dataSourceInfo);
        if (!metaCompatible) {
            errorMsg = "Detected that META is not compatible now!";
            LOG.error(errorMsg);
            //出错了，善后事宜
            FullPullHelper.updatePendingTasksTrackInfo(zkService, dsName, dataSourceInfo, DataPullConstants.FULLPULL_PENDING_TASKS_OP_REMOVE_WATCHING);
            FullPullHelper.finishPullReport(zkService, dataSourceInfo, FullPullHelper.getCurrentTimeStampString(), Constants.DataTableStatus.DATA_STATUS_ABORT, errorMsg);
            collector.fail(input);
            return;
        }

        String dsKey = FullPullHelper.getDataSourceKey(JSONObject.parseObject(dataSourceInfo));
        DBConfiguration dbConf = FullPullHelper.getDbConfiguration(dataSourceInfo);
        DataDrivenDBInputFormat inputFormat = new DataDrivenDBInputFormat();
        inputFormat.setConf(dbConf);
        GenericJdbcManager dbManager = null;

        try {
            dbManager = FullPullHelper.getDbManager(dbConf, dbConf.getString(DBConfiguration.DataSourceInfo.URL_PROPERTY_READ_ONLY));

            //获取分片列
            int splitShardSize = dbConf.getSplitShardSize();
            String splitByCol = "";
            // 如果splitShardSize 为 -1，不分片
            if (splitShardSize != -1) {
                splitByCol = DBHelper.getSplitColumn(dbManager, dbConf);
            }
            LOG.info("doSplitting() Will use col [{}] to split data.", splitByCol);
            //把分片列更新到全量历史库
            JSONObject fullpullUpdateParams = new JSONObject();
            fullpullUpdateParams.put("dataSourceInfo",dataSourceInfo);
            fullpullUpdateParams.put("split_column",splitByCol);
            FullPullHelper.writeStatusToDbManager(fullpullUpdateParams);
            // oracleManager.checkTableImportOptions();
            //根据分片列获取分片信息
            Map<String, Object> splitInfoMap = inputFormat.getSplits(splitByCol, dbManager, dataSourceInfo, zkService);
            //从分片信息中获取总行数
            long totalRows = (long) splitInfoMap.get(Constants.TABLE_SPLITTED_TOTAL_ROWS_KEY);
            //int splitsCount= (int)splitInfoMap.get(Constants.TABLE_SPLITTED_SHARDS_COUNT_KEY);

            JSONObject wrapperObj = JSONObject.parseObject(dataSourceInfo);
            JSONObject payloadObj = wrapperObj.getJSONObject("payload");
            String version = payloadObj.getString(DataPullConstants.FullPullInterfaceJson.VERSION_KEY);
            String batchNo = payloadObj.getString(DataPullConstants.FullPullInterfaceJson.BATCH_NO_KEY);

            //分片信息列表
            List<InputSplit> inputSplitList = (List<InputSplit>) splitInfoMap.get(Constants.TABLE_SPLITTED_SHARD_SPLITS_KEY);
            //总共分为多少片
            int splitsCount = inputSplitList.size();
            //向monitor节点写分片信息
            writeTotalCountToZk(zkService, dataSourceInfo, splitsCount, totalRows);

            //包装每一片信息，写kafka，供数据拉取进程使用
            int splitIndex = 0;
            Long firstShardMsgOffset = null;
            Long lastShardMsgOffset = null;
            RecordMetadata producedRecord = null;
            for (InputSplit inputSplit : inputSplitList) {
                JSONObject wrapperObject = new JSONObject();
                wrapperObject.put(DataPullConstants.DATA_SOURCE_INFO, dataSourceInfo);
                wrapperObject.put(DataPullConstants.DATA_CHUNK_COUNT, splitsCount);
                wrapperObject.put(DataPullConstants.ZkMonitoringJson.DB_NAMESPACE_NODE_TOTAL_ROWS, totalRows);

                JSONObject inputSplitJsonObject = (JSONObject) JSONObject.toJSON(inputSplit);
                wrapperObject.put(DataPullConstants.DATA_CHUNK_SPLIT, inputSplitJsonObject);
                wrapperObject.put(DataPullConstants.DATA_CHUNK_SPLIT_INDEX, ++splitIndex);

                String fullPullMediantTopic = commonProps.getProperty(Constants.ZkTopoConfForFullPull.FULL_PULL_MEDIANT_TOPIC);
                ProducerRecord record = new ProducerRecord<>(fullPullMediantTopic, dataType, wrapperObject.toString().getBytes());
                Future<RecordMetadata> future = byteProducer.send(record);
                producedRecord = future.get();
                if (splitIndex == 1) {
                    firstShardMsgOffset = producedRecord.offset();
                }
                if (splitIndex == inputSplitList.size()) {
                    lastShardMsgOffset = producedRecord.offset();
                }
                LOG.info("dskey: {}, 生成第{}片分片, 所属分区：{}.{}, lower:{}, upper:{}", dsKey, splitIndex,
                        inputSplit.getTargetTableName(), inputSplit.getTablePartitionInfo(),
                        ((DataDrivenDBInputFormat.DataDrivenDBInputSplit) inputSplit).getLowerValue(),
                        ((DataDrivenDBInputFormat.DataDrivenDBInputSplit) inputSplit).getUpperValue());
            }
            fullpullUpdateParams.clear();
            fullpullUpdateParams.put("dataSourceInfo", dataSourceInfo);
            fullpullUpdateParams.put("version", version);
            fullpullUpdateParams.put("batch_id", batchNo);
            fullpullUpdateParams.put("total_partition_count", splitsCount);
            fullpullUpdateParams.put("total_row_count", totalRows);
            fullpullUpdateParams.put("status", "splitting");
            fullpullUpdateParams.put("first_shard_msg_offset", firstShardMsgOffset);
            fullpullUpdateParams.put("last_shard_msg_offset", lastShardMsgOffset);
            FullPullHelper.writeStatusToDbManager(fullpullUpdateParams);
            collector.ack(input);
            LOG.info("{}:生成分片完毕，总共分为{}片", dsKey, splitsCount);

            // LOG.info("..............FullPullHelper 生成分片完毕 start..........");
            FullPullHelper.updatePendingTasksTrackInfo(zkService, dsName, dataSourceInfo, DataPullConstants.FULLPULL_PENDING_TASKS_OP_REMOVE_WATCHING);
            // LOG.info("..............FullPullHelper 生成分片完毕 end............");

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            errorMsg = "Exception happened when splitting data shards." + e.getMessage();
            LOG.error(errorMsg, e);

            //出错了，善后事宜
            FullPullHelper.updatePendingTasksTrackInfo(zkService, dsName, dataSourceInfo, DataPullConstants.FULLPULL_PENDING_TASKS_OP_REMOVE_WATCHING);
            FullPullHelper.finishPullReport(zkService, dataSourceInfo, FullPullHelper.getCurrentTimeStampString(), Constants.DataTableStatus.DATA_STATUS_ABORT, errorMsg);
            collector.fail(input);
        } finally {
            try {
                if (dbManager != null) {
                    dbManager.close();
                }
            } catch (Exception e) {
                LOG.error("close dbManager error.", e);
            }
        }
    }

    public void stopTopo(Tuple input) {
        String data = (String) input.getValue(0);
        try {
            String fullPullMediantTopic = commonProps.getProperty(Constants.ZkTopoConfForFullPull.FULL_PULL_MEDIANT_TOPIC);
            ProducerRecord record = new ProducerRecord<>(fullPullMediantTopic, DataPullConstants.COMMAND_FULL_PULL_STOP, data.getBytes());
            byteProducer.send(record);
            Future<RecordMetadata> future = byteProducer.send(record);
            RecordMetadata producedRecord = null;
            producedRecord = future.get();
            LOG.info("收到停服命令:{},转发给拉取Topology", data);

            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

            //Call storm api to kill topo
            int topoKillWaitTime = Constants.ZkTopoConfForFullPull.TOPOS_KILL_STORM_API_WAITTIME_PARAM_DEFAULT_VAL;
            String topoKillWaitTimeParam = commonProps.getProperty(Constants.ZkTopoConfForFullPull.TOPOS_KILL_STORM_API_WAITTIME_PARAM);
            try {
                topoKillWaitTime = Integer.valueOf(topoKillWaitTimeParam);
            } catch (Exception e) {
                // Just ignore, use the default value
            }

            String killResult = CommandCtrl.killTopology(zkService, topologyId, topoKillWaitTime);
            // TODO Topo都kill了，后面这几行代码还能执行？
            LOG.info("Id为 {}的Topology Kill已結束.", topologyId);
            // collector.ack(input);  //不跟踪消息
        } catch (Exception e) {
            LOG.error("收到停服命令:{},处理时发生异常！", data);
            collector.fail(input);
        }
    }

    private void writeTotalCountToZk(ZkService zkService, String dataSourceInfo, int totalCount, long totalRows) {
        String currentTimeStampString = FullPullHelper.getCurrentTimeStampString();

        String progressInfoNodePath = FullPullHelper.getMonitorNodePath(dataSourceInfo);
        // 当前拉取全量对应的具体监控节点的更新
        try {
            ProgressInfo progressObj = FullPullHelper.getMonitorInfoFromZk(zkService, progressInfoNodePath);
            progressObj.setUpdateTime(currentTimeStampString);
            progressObj.setTotalCount(String.valueOf(totalCount));
            progressObj.setPartitions(String.valueOf(totalCount));
            progressObj.setTotalRows(String.valueOf(totalRows));
            ObjectMapper mapper = JsonUtil.getObjectMapper();
            FullPullHelper.updateZkNodeInfo(zkService, progressInfoNodePath, mapper.writeValueAsString(progressObj));
        } catch (Exception e) {
            String errorMsg = "Encountered exception when writing msg to zk monitor.";
            LOG.error(errorMsg, e);
        }
    }

    private void loadRunningConf(String reloadMsgJson) {
        String notifyEvtName = reloadMsgJson == null ? "loaded" : "reloaded";
        String loadResultMsg = null;
        try {
            this.confMap = FullPullHelper.loadConfProps(zkconnect, topologyId, zkTopoRoot, null);
            this.commonProps = (Properties) confMap.get(FullPullHelper.RUNNING_CONF_KEY_COMMON);
            this.dsName = commonProps.getProperty(Constants.ZkTopoConfForFullPull.DATASOURCE_NAME);
            this.byteProducer = (Producer) confMap.get(FullPullHelper.RUNNING_CONF_KEY_BYTE_PRODUCER);
            this.zkService = (ZkService) confMap.get(FullPullHelper.RUNNING_CONF_KEY_ZK_SERVICE);
            loadResultMsg = "Running Config is " + notifyEvtName + " successfully for DataShardsSplittingBolt!";
            LOG.info(loadResultMsg);
        } catch (Exception e) {
            loadResultMsg = e.getMessage();
            LOG.error(notifyEvtName + "ing running configuration encountered Exception!", loadResultMsg);
            throw e;
        } finally {
            if (reloadMsgJson != null) {
                FullPullHelper.saveReloadStatus(reloadMsgJson, "splitting-bolt", false, zkconnect);
            }
        }
    }
}
