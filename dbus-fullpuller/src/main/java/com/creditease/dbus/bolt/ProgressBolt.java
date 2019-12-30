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


package com.creditease.dbus.bolt;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.common.FullPullConstants;
import com.creditease.dbus.common.bean.ProgressInfo;
import com.creditease.dbus.common.bean.ProgressInfoParam;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.ZkService;
import com.creditease.dbus.commons.exception.InitializationException;
import com.creditease.dbus.helper.FullPullHelper;
import com.creditease.dbus.utils.TimeUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;


/**
 * 此bolt只能启一个
 */
public class ProgressBolt extends BaseRichBolt {
    private static final long serialVersionUID = -464903510457397266L;
    private Logger logger = LoggerFactory.getLogger(getClass());

    private final String zkTopoRoot = FullPullConstants.TOPOLOGY_ROOT + "/" + FullPullConstants.FULL_PULLING_PROPS_ROOT;

    private OutputCollector collector;
    private String zkConnect;
    private String topologyId;
    private String dsName;
    private Properties commonProps;
    private Producer byteProducer;
    ZkService zkService = null;
    //private Map<String, ProgressInfo> progressInfoMap = new HashMap<>();
    private Map confMap;
    private long lastReloadTime;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.zkConnect = (String) conf.get(FullPullConstants.ZKCONNECT);
        this.topologyId = (String) conf.get(FullPullConstants.FULL_PULLER_TOPOLOGY_ID);
        this.dsName = (String) conf.get(FullPullConstants.DS_NAME);

        try {
            //设置topo类型，用于获取配置信息路径
            FullPullHelper.setTopologyType(FullPullConstants.FULL_PULLER_TYPE);
            loadRunningConf(null);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new InitializationException();
        }
    }

    @Override
    public void execute(Tuple input) {
        JSONObject progressInfoJson = (JSONObject) input.getValueByField("progressInfo");
        String reqString = progressInfoJson.getString(FullPullConstants.FULLPULL_REQ_PARAM);
        JSONObject reqJson = JSONObject.parseObject(reqString);
        String dataType = reqJson.getString(FullPullConstants.REQ_TYPE);
        if (null == dataType) {
            logger.error("the type of request is null on ProgressBolt!");
            this.collector.fail(input);
            return;
        }
        try {
            if (dataType.equals(FullPullConstants.COMMAND_FULL_PULL_RELOAD_CONF)) {
                logger.info("[progress bolt] receive config reloading request :{}", progressInfoJson);
                //防止reload消息重复处理,10分钟内仅处理一次reload
                if ((System.currentTimeMillis() - lastReloadTime) < 600000) {
                    return;
                }
                destory();
                loadRunningConf(reqString);
                this.lastReloadTime = System.currentTimeMillis();
                logger.info("[progress bolt] config for progress bolt is reloaded successfully!");
            } else if (dataType.equals(FullPullConstants.COMMAND_FULL_PULL_FINISH_REQ)) {
                return;
            } else {
                String progressInfoNodePath = null;
                String dsKey = null;
                try {
                    progressInfoNodePath = progressInfoJson.getString(FullPullConstants.DATA_MONITOR_ZK_PATH);
                    dsKey = FullPullHelper.getDataSourceKey(reqJson);
                    long finishedRows = Long.parseLong(progressInfoJson.getString(FullPullConstants.DB_NAMESPACE_NODE_FINISHED_ROWS));
                    long finishedCount = Long.parseLong(progressInfoJson.getString(FullPullConstants.DB_NAMESPACE_NODE_FINISHED_COUNT));
                    ProgressInfo progressInfo = updateMonitorFinishPartitionInfo(reqString, progressInfoNodePath, finishedRows, finishedCount, dsKey);

                    if (isFinished(progressInfo, reqJson)) {
                        //如果取不到progress信息或者已经出现错误，跳过后来的tuple数据
                        if (progressInfo.getErrorMsg() != null) {
                            //如果出错的话，就不设置ending状态了, 写一下结束时间,不发结束报告
                            ProgressInfoParam infoParam = new ProgressInfoParam();
                            infoParam.setEndTime(TimeUtils.getCurrentTimeStampString());
                            FullPullHelper.updateZkNodeInfoWithVersion(zkService, progressInfoNodePath, infoParam);
                            FullPullHelper.finishPullReport(reqString, null, null);
                            logger.info("[progress bolt] {}:此次全量拉取发生异常！{}", dsKey, progressInfo.getErrorMsg());
                        } else {
                            //如果没有错误，就完成后续工作
                            ProgressInfoParam infoParam = new ProgressInfoParam();
                            infoParam.setEndTime(TimeUtils.getCurrentTimeStampString());
                            infoParam.setStatus(FullPullConstants.FULL_PULL_STATUS_ENDING);
                            FullPullHelper.updateZkNodeInfoWithVersion(zkService, progressInfoNodePath, infoParam);
                            FullPullHelper.finishPullReport(reqString, FullPullConstants.FULL_PULL_STATUS_ENDING, null);
                            logger.info("[progress bolt] {}:此次全量拉取处理完成！", dsKey);
                        }
                        deleteProgressInfo(progressInfoNodePath);
                        sendFinishMsgToSpout(reqJson);
                    }
                    this.collector.ack(input);
                } catch (Exception e) {
                    String errorMsg = dsKey + ":Exception happened when updating zookeeper split info: " + e.getMessage();
                    logger.error(errorMsg, e);
                    FullPullHelper.finishPullReport(reqString, null, errorMsg);
                    deleteProgressInfo(progressInfoNodePath);
                    this.collector.fail(input);
                }
            }
        } catch (Exception e) {
            logger.error("Exception happended on process bolt execute()", e);
        }
    }

    private void sendFinishMsgToSpout(JSONObject reqJson) throws Exception {
        reqJson.put("type", FullPullConstants.COMMAND_FULL_PULL_FINISH_REQ);
        JSONObject wrapperJson = new JSONObject();
        wrapperJson.put(FullPullConstants.FULLPULL_REQ_PARAM, reqJson.toJSONString());
        ProducerRecord record = new ProducerRecord<>(dsName + "_callback", FullPullConstants.COMMAND_FULL_PULL_FINISH_REQ, wrapperJson.toString().getBytes());
        Future<RecordMetadata> future = byteProducer.send(record);
        logger.info("send full pull finish msg to pull spout offset is {}", future.get().offset());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }

    private ProgressInfo getProgressInfo(String dsKey, String progressInfoNodePath) throws Exception {
        return FullPullHelper.getMonitorInfoFromZk(this.zkService, progressInfoNodePath);
        //ProgressInfo progressObj = progressInfoMap.get(progressInfoNodePath);
        //if (progressObj == null || progressObj.getTotalCount() == null || progressObj.getTotalRows() == null) {
        //    try {
        //        progressObj = FullPullHelper.getMonitorInfoFromZk(this.zkService, progressInfoNodePath);
        //    } catch (Exception e) {
        //        String errMsg = dsKey + "ProgressBolt get progress info from zk exception!";
        //        logger.error(errMsg, e);
        //    }
        //    this.progressInfoMap.put(progressInfoNodePath, progressObj);
        //}
        //return progressObj;
    }

    private void deleteProgressInfo(String progressInfoNodePath) {
        //if (progressInfoNodePath != null) {
        //    this.progressInfoMap.remove(progressInfoNodePath);
        //}
    }

    private ProgressInfo updateMonitorFinishPartitionInfo(String reqString, String progressInfoNodePath, long finishedRows,
                                                          long finishedCount, String dsKey) throws Exception {
        ProgressInfoParam infoParam = new ProgressInfoParam();
        infoParam.setFinishedRows(finishedRows);
        infoParam.setFinishedCount(finishedCount);
        infoParam.setStatus(FullPullConstants.FULL_PULL_STATUS_PULLING);
        ProgressInfo progressInfo = FullPullHelper.updateZkNodeInfoWithVersion(zkService, progressInfoNodePath, infoParam);
        FullPullHelper.updateStatusToFullPullHistoryTable(progressInfo, FullPullHelper.getSeqNo(reqString), null, null, null);
        logger.info("[progress bolt] 更新处理进度信息: {}:总片数{}片，已完成{}片，总行数{}条，已完成{}条，耗时{}秒", dsKey,
                progressInfo.getTotalCount(), progressInfo.getFinishedCount(), progressInfo.getTotalRows(), progressInfo.getFinishedRows(),
                progressInfo.getConsumeSecs());
        return progressInfo;
    }

    private boolean isFinished(ProgressInfo objProgInfo, JSONObject reqJson) throws Exception {
        if (!Constants.FULL_PULL_STATUS_ENDING.equals(objProgInfo.getSplitStatus())) {
            return false;
        }
        long totalCount = Long.parseLong(objProgInfo.getTotalCount());
        long finishedCount = Long.parseLong(objProgInfo.getFinishedCount());
        if (finishedCount >= totalCount) {
            return true;
        }
        return false;
    }

    private void loadRunningConf(String reloadMsgJson) throws Exception {
        String notifyEvtName = reloadMsgJson == null ? "loaded" : "reloaded";
        try {
            this.confMap = FullPullHelper.loadConfProps(this.zkConnect, this.topologyId, this.dsName, this.zkTopoRoot, "PullProcessBolt");
            this.zkService = (ZkService) this.confMap.get(FullPullHelper.RUNNING_CONF_KEY_ZK_SERVICE);
            this.commonProps = (Properties) this.confMap.get(FullPullHelper.RUNNING_CONF_KEY_COMMON);
            this.byteProducer = (Producer) this.confMap.get(FullPullHelper.RUNNING_CONF_KEY_BYTE_PRODUCER);
            logger.info("[progress bolt] Running Config is " + notifyEvtName + " successfully for ProgressBolt!");
        } catch (Exception e) {
            logger.error(notifyEvtName + "ing running configuration encountered Exception!", e);
            throw e;
        } finally {
            FullPullHelper.saveReloadStatus(reloadMsgJson, "pulling-progress-bolt", false, this.zkConnect);
        }
    }

    private void destory() {
        if (byteProducer != null) {
            byteProducer.close();
            byteProducer = null;
        }
        logger.info("close byteProducer");
    }
}
