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
import com.creditease.dbus.common.bean.DBConfiguration;
import com.creditease.dbus.commons.ZkService;
import com.creditease.dbus.commons.exception.InitializationException;
import com.creditease.dbus.handler.SplitHandler;
import com.creditease.dbus.helper.FullPullHelper;
import com.creditease.dbus.utils.TimeUtils;
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

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

public class DataShardsSplittingBolt extends BaseRichBolt {
    private static final long serialVersionUID = -6908653804104991205L;
    private Logger logger = LoggerFactory.getLogger(getClass());

    private String topologyId;
    private String zkconnect;
    private String dsName;
    private String zkTopoRoot;
    private Map confMap;
    private OutputCollector collector;

    private Properties commonProps;
    private Producer byteProducer;
    private ZkService zkService;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.topologyId = (String) conf.get(FullPullConstants.FULL_SPLITTER_TOPOLOGY_ID);
        this.dsName = (String) conf.get(FullPullConstants.DS_NAME);
        this.zkconnect = (String) conf.get(FullPullConstants.ZKCONNECT);
        this.zkTopoRoot = FullPullConstants.TOPOLOGY_ROOT + "/" + FullPullConstants.FULL_SPLITTING_PROPS_ROOT;
        try {
            //设置topo类型，用于获取配置信息路径
            FullPullHelper.setTopologyType(FullPullConstants.FULL_SPLITTER_TYPE);
            // 初始化配置文件
            loadRunningConf(null);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new InitializationException(e);
        }
    }

    @Override
    public void execute(Tuple input) {
        String data = (String) input.getValue(0);
        String dataType = JSONObject.parseObject(data).getString("type");
        if (dataType == null) {
            logger.error("dataType is null in DataShardsSplittingBolt");
            this.collector.fail(input);
            return;
        }

        try {
            if (dataType.equals(FullPullConstants.COMMAND_FULL_PULL_RELOAD_CONF)) {
                logger.info("[split bolt] receive config reloading request :{}", data);
                destory();
                //重新加载配置
                loadRunningConf(data);
                //将reload请求通过中间topic传导到puller
                String mediantTopic = commonProps.getProperty(FullPullConstants.FULL_PULL_MEDIANT_TOPIC);
                String reqString = (String) input.getValue(0);
                JSONObject wrapperJson = new JSONObject();
                wrapperJson.put(FullPullConstants.FULLPULL_REQ_PARAM, reqString);
                ProducerRecord<String, byte[]> producedRecord = new ProducerRecord<>(mediantTopic, FullPullConstants.COMMAND_FULL_PULL_RELOAD_CONF, wrapperJson.toString().getBytes());
                Future<RecordMetadata> future = byteProducer.send(producedRecord);
                logger.info("[split bolt] send reload config msg to mediant topic success , topic:{} ,offset:{}", mediantTopic, future.get().offset());
                logger.info("[split bolt] config for split bolt is reloaded successfully!");
            } else if (dataType.equals(FullPullConstants.DATA_EVENT_INDEPENDENT_FULL_PULL_REQ)) {
                String reqString = (String) input.getValue(0);
                String errorMsg = "";
                JSONObject reqJson = JSONObject.parseObject(reqString);
                String dsKey = FullPullHelper.getDataSourceKey(reqJson);
                try {
                    if (FullPullHelper.hasErrorMessage(zkService, reqString)) {
                        this.collector.fail(input);
                        return;
                    }

                    DBConfiguration dbConf = FullPullHelper.getDbConfiguration(reqString);
                    boolean metaCompatible = FullPullHelper.validateMetaCompatible(reqString);
                    if (!metaCompatible) {
                        errorMsg = "Detected that META is not compatible now!";
                        logger.error(errorMsg);
                        throw new RuntimeException();
                    }
                    Long startTime = System.currentTimeMillis();
                    logger.info("[split bolt] {}, doSplitting data start {}", dsKey, startTime);
                    SplitHandler splitHandler = FullPullHelper.getSplitHandler(dbConf.getString(DBConfiguration.DataSourceInfo.DS_TYPE));
                    splitHandler.setDbConf(dbConf);
                    splitHandler.setReqString(reqString);
                    splitHandler.setZkService(this.zkService);
                    splitHandler.setCommonProps(this.commonProps);
                    splitHandler.setByteProducer(this.byteProducer);
                    splitHandler.setDataType(dataType);

                    splitHandler.executeSplit();
                    logger.info("[split bolt] {}, split complete , cost time {}", dsKey, TimeUtils.formatTime(System.currentTimeMillis() - startTime));
                    collector.ack(input);
                } catch (Exception e) {
                    errorMsg = String.format("split error! dsKey: %s, %s; %s", dsKey, errorMsg, e.getMessage());
                    logger.error(errorMsg, e);
                    FullPullHelper.finishPullReport(reqString, FullPullConstants.FULL_PULL_STATUS_ABORT, errorMsg);
                    this.collector.fail(input);
                }
            }
        } catch (Exception e) {
            logger.error("Exception happended on data shard split bolt execute()", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    private void loadRunningConf(String reloadMsgJson) {
        String notifyEvtName = reloadMsgJson == null ? "loaded" : "reloaded";
        String loadResultMsg = null;
        try {
            this.confMap = FullPullHelper.loadConfProps(this.zkconnect, this.topologyId, this.dsName, this.zkTopoRoot, "SplitBolt");
            this.commonProps = (Properties) this.confMap.get(FullPullHelper.RUNNING_CONF_KEY_COMMON);
            this.byteProducer = (Producer) this.confMap.get(FullPullHelper.RUNNING_CONF_KEY_BYTE_PRODUCER);
            this.zkService = (ZkService) this.confMap.get(FullPullHelper.RUNNING_CONF_KEY_ZK_SERVICE);
            loadResultMsg = "Running Config is " + notifyEvtName + " successfully for DataShardsSplittingBolt!";
            logger.info(loadResultMsg);
        } catch (Exception e) {
            logger.error(notifyEvtName + "ing running configuration encountered Exception!");
            throw e;
        } finally {
            if (reloadMsgJson != null) {
                FullPullHelper.saveReloadStatus(reloadMsgJson, "splitting-bolt", false, this.zkconnect);
            }
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
