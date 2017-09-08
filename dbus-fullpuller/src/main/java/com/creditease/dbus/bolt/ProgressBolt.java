/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2017 Bridata
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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.common.DataPullConstants;
import com.creditease.dbus.common.FullPullHelper;
import com.creditease.dbus.common.ProgressInfo;
import com.creditease.dbus.common.utils.JsonUtil;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.Constants.ZkTopoConfForFullPull;
import com.creditease.dbus.commons.ZkService;
import com.creditease.dbus.commons.exception.InitializationException;
import com.creditease.dbus.spout.DataPullingSpout;
import com.fasterxml.jackson.databind.ObjectMapper;


/**
 * 此bolt只能启一个
 */
public class ProgressBolt extends BaseRichBolt {
    private Logger LOG = LoggerFactory.getLogger(getClass());

    private final String zkTopoRoot = Constants.TOPOLOGY_ROOT + "/" + Constants.FULL_PULLING_PROPS_ROOT;

    private OutputCollector collector;
    private String zkConnect;
    private String topologyId;
    private boolean initialized = false;
    //是否是独立拉全量
    private boolean isGlobal;
    private String zkMonitorRootNodePath;
    private String dsName;
    ZkService zkService = null;
    private Map<String,ProgressInfo> progressInfoMap = new HashMap<String,ProgressInfo>();
    private Map confMap;
    private Properties commonProps;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        this.zkConnect = (String) conf.get(Constants.StormConfigKey.ZKCONNECT);
        this.topologyId = (String) conf.get(Constants.StormConfigKey.FULL_PULLER_TOPOLOGY_ID);

        this.isGlobal = this.topologyId.toLowerCase().indexOf(ZkTopoConfForFullPull.GLOBAL_FULLPULLER_TOPO_PREFIX) != -1 ? true : false;
        if (this.isGlobal) {
            this.zkMonitorRootNodePath = Constants.FULL_PULL_MONITOR_ROOT_GLOBAL;
        } else {
            this.zkMonitorRootNodePath = Constants.FULL_PULL_MONITOR_ROOT;
        }

        loadRunningConf(null);
    }

    public void execute(Tuple input) {
        String dsKey = null;
        String dataSourceInfo = null;
        String dbNameSpace = null;
        try {
            JSONObject jsonObj = (JSONObject) input.getValueByField("progressInfo");
            dataSourceInfo = jsonObj.getString(DataPullConstants.DATA_SOURCE_INFO);
            String cmdType = ((JSONObject)JSONObject.parse(dataSourceInfo)).getString("type");
            if(null == cmdType) {
                LOG.error("the type of request is null on PagedBatchDataFetchingBolt!");
                collector.fail(input);
                return;

            } else if (cmdType.equals(DataPullConstants.COMMAND_FULL_PULL_STOP)) {
                LOG.error("Impossible to be here!!! the type of request is COMMAND_FULL_PULL_STOP on PagedBatchDataFetchingBolt!");
                return;

            } else if (cmdType.equals(DataPullConstants.COMMAND_FULL_PULL_RELOAD_CONF)) {
                //处理reload事件
                loadRunningConf(dataSourceInfo);
                //command 不用ack, 不跟踪
                return;
            }

            dbNameSpace = jsonObj.getString(DataPullConstants.DATA_SOURCE_NAME_SPACE);
            long dealRows = Long.parseLong(jsonObj.getString(DataPullConstants.ZkMonitoringJson.DB_NAMESPACE_NODE_FINISHED_ROWS));
            long finishedCount = Long.parseLong(jsonObj.getString(DataPullConstants.ZkMonitoringJson.DB_NAMESPACE_NODE_FINISHED_COUNT));
            String totalRows = jsonObj.getString(DataPullConstants.ZkMonitoringJson.DB_NAMESPACE_NODE_TOTAL_ROWS);
            String startSecs = jsonObj.getString(DataPullConstants.ZkMonitoringJson.DB_NAMESPACE_NODE_START_SECS);
            String totalPartitions = jsonObj.getString(DataPullConstants.DATA_CHUNK_COUNT);

            dsKey = FullPullHelper.getDataSourceKey(JSONObject.parseObject(dataSourceInfo));
            ProgressInfo objProgInfo = getProgressInfo(dsKey, dbNameSpace, totalRows, startSecs, totalPartitions);
            setProgressInfo(objProgInfo, dealRows, finishedCount);
            if (isFinished(objProgInfo)) {
                //如果取不到progress信息或者已经出现错误，跳过后来的tuple数据
                ProgressInfo progressInfo = FullPullHelper.getMonitorInfoFromZk(zkService, dbNameSpace);
                if (progressInfo.getErrorMsg() != null) {
                    //如果出错的话，
                    // 1 就不设置ending状态了, 写一下结束时间
                    // 2 不发结束报告
                    objProgInfo.setEndTime(FullPullHelper.getCurrentTimeStampString());
                    FullPullHelper.updateMonitorFinishPartitionInfo(zkService, dbNameSpace, objProgInfo);

                } else {
                    //如果没有错误，就完成后续工作
                    objProgInfo.setEndTime(FullPullHelper.getCurrentTimeStampString());
                    objProgInfo.setStatus(Constants.FULL_PULL_STATUS_ENDING);
                    FullPullHelper.updateMonitorFinishPartitionInfo(zkService, dbNameSpace, objProgInfo);

                    FullPullHelper.finishPullReport(zkService, dataSourceInfo, objProgInfo.getEndTime(),
                            Constants.DataTableStatus.DATA_STATUS_OK, null);
                    FullPullHelper.updatePendingTasksTrackInfo(zkService, dsName, dataSourceInfo, DataPullConstants.FULLPULL_PENDING_TASKS_OP_REMOVE_WATCHING);
                    LOG.info("{}:此次全量拉取处理完成！", dsKey);
                }

                deleteProgressInfo(dbNameSpace);
            } else {
                FullPullHelper.updateMonitorFinishPartitionInfo(zkService, dbNameSpace, objProgInfo);
            }
            printProgressInfo(dsKey, objProgInfo);

            collector.ack(input);
        } catch (Exception e) {
            String errorMsg = dsKey + ":Exception happened when updating zookeeper split info: " + e.getMessage();
            LOG.error(errorMsg, e);
            FullPullHelper.finishPullReport(zkService, dataSourceInfo, FullPullHelper.getCurrentTimeStampString(),
                    Constants.DataTableStatus.DATA_STATUS_ABORT, errorMsg);
            deleteProgressInfo(dbNameSpace);
            collector.fail(input);
        }
    }

     public void declareOutputFields(OutputFieldsDeclarer declarer) {
         declarer.declare(new Fields("message"));
     }

    private ProgressInfo getProgressInfo(String dsKey, String dbNameSpace, String totalRows, String startSecs, String totalPartitions){
        ProgressInfo progressObj = progressInfoMap.get(dbNameSpace);
        if(progressObj == null) {
            try {
                progressObj = FullPullHelper.getMonitorInfoFromZk(zkService, dbNameSpace);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                String errMsg = dsKey + "ProgressBolt get progress info from zk exception!";
                LOG.error(errMsg, e);
            }

            if(startSecs != null && !startSecs.equals(progressObj.getStartSecs())) {
                progressObj.setStartSecs(startSecs);
            }
            if(totalRows != null && !totalRows.equals(progressObj.getTotalRows())) {
                progressObj.setTotalRows(totalRows);
            }
            if(totalPartitions != null && !totalPartitions.equals(progressObj.getTotalCount())) {
                progressObj.setTotalCount(totalPartitions);
            }
            progressInfoMap.put(dbNameSpace, progressObj);
        }
        return progressObj;
    }

    private void deleteProgressInfo(String dbNameSpace){
         progressInfoMap.remove(dbNameSpace);
     }

    private void setProgressInfo(ProgressInfo objProgInfo, long dealRows, long finishedCount) {
        String currentTimeStampString = FullPullHelper.getCurrentTimeStampString();
        long curSecs = System.currentTimeMillis() / 1000;
        long startSecs = objProgInfo.getStartSecs() == null?curSecs:Long.parseLong(objProgInfo.getStartSecs());
        long consumeSecs = curSecs - startSecs;
        long curDealRows = objProgInfo.getFinishedRows() == null?dealRows:(dealRows + Long.parseLong(objProgInfo.getFinishedRows()));
        long curFinishedCount = objProgInfo.getFinishedCount() == null?finishedCount:(Long.parseLong(objProgInfo.getFinishedCount()) + finishedCount);
        objProgInfo.setUpdateTime(currentTimeStampString);
        objProgInfo.setFinishedRows(String.valueOf(curDealRows));
        objProgInfo.setFinishedCount(String.valueOf(curFinishedCount));
        objProgInfo.setConsumeSecs(String.valueOf(consumeSecs) + "s");
     }

     private boolean isFinished(ProgressInfo objProgInfo) {
         long totalCount = Long.parseLong(objProgInfo.getTotalCount());
         long finishedCount = Long.parseLong(objProgInfo.getFinishedCount());
         if(finishedCount >= totalCount) {
             return true;
         }
         return false;
     }

     private void printProgressInfo(String dsKey, ProgressInfo objProgInfo) {
         String totalRows = objProgInfo.getTotalRows();
         String finishedRows = objProgInfo.getFinishedRows();
         String totalCount = objProgInfo.getTotalCount();
         String finishedCount = objProgInfo.getFinishedCount();
         String consumeSecs = objProgInfo.getConsumeSecs();
         LOG.info("更新处理进度信息: {}:总片数{}，已完成{}片，总行数{}，已完成{}行，耗时{}", dsKey, totalCount, finishedCount, totalRows, finishedRows, consumeSecs);
     }



    private void loadRunningConf(String reloadMsgJson) {
        String notifyEvtName = reloadMsgJson == null ? "loaded" : "reloaded";
        try {
            this.confMap = FullPullHelper.loadConfProps(zkConnect, topologyId, zkTopoRoot, null);
            this.commonProps = (Properties) confMap.get(FullPullHelper.RUNNING_CONF_KEY_COMMON);
            this.dsName = commonProps.getProperty(Constants.ZkTopoConfForFullPull.DATASOURCE_NAME);
            this.zkService = (ZkService) confMap.get(FullPullHelper.RUNNING_CONF_KEY_ZK_SERVICE);

            LOG.info("Running Config is " + notifyEvtName + " successfully for ProgressBolt!");
        } catch (Exception e) {
            LOG.error(notifyEvtName + "ing running configuration encountered Exception!", e);
            throw e;
        } finally {
            FullPullHelper.saveReloadStatus(reloadMsgJson, "pulling-progress-bolt", false, zkConnect);
        }
    }
}
