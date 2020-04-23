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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.common.FullPullConstants;
import com.creditease.dbus.common.FullPullPluginLoader;
import com.creditease.dbus.common.bean.DBConfiguration;
import com.creditease.dbus.common.bean.HdfsConnectInfo;
import com.creditease.dbus.common.format.InputSplit;
import com.creditease.dbus.commons.ZkService;
import com.creditease.dbus.commons.exception.InitializationException;
import com.creditease.dbus.handler.PullHandler;
import com.creditease.dbus.helper.FullPullHelper;
import com.creditease.dbus.msgencoder.PluginManagerProvider;
import com.creditease.dbus.utils.TimeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.Producer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;


public class PagedBatchDataFetchingBolt extends BaseRichBolt {
    private static final long serialVersionUID = 1735002584169069205L;
    private Logger logger = LoggerFactory.getLogger(getClass());

    private OutputCollector collector;
    private TopologyContext context;
    private String topologyId;
    private String zkconnect;
    private String dsName;
    private String zkTopoRoot;
    private Map confMap;
    //每次发送给kafka的数据最大值1M
    private Long kafkaSendBatchSize = 1048576L;
    //每次发送给hdfs的数据最大值4M
    private Long hdfsSendBatchSize = 1048576L * 4;
    //每次发给kafka的行数，与kafkaSendBatchSize配合使用，谁先满足条件，谁就生效
    private Long kafkaSendRows = 1000L;
    //hdfs文件最大值128M
    private Long hdfsFileMaxSize = 134217728L;

    private Properties stringProducerProps;
    private Properties hdfsConfigProps;
    private Producer stringProducer;
    private ZkService zkService;
    private FileSystem fileSystem;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.topologyId = (String) conf.get(FullPullConstants.FULL_PULLER_TOPOLOGY_ID);
        this.dsName = (String) conf.get(FullPullConstants.DS_NAME);
        this.zkconnect = (String) conf.get(FullPullConstants.ZKCONNECT);
        this.zkTopoRoot = FullPullConstants.TOPOLOGY_ROOT + "/" + FullPullConstants.FULL_PULLING_PROPS_ROOT;
        this.context = context;
        try {
            //设置topo类型，用于获取配置信息路径
            FullPullHelper.setTopologyType(FullPullConstants.FULL_PULLER_TYPE);
            loadRunningConf(null);
            logger.info("[pull bolt] init complete!");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new InitializationException();
        }
    }

    @Override
    public void execute(Tuple input) {
        String inputString = (String) input.getValue(0);
        JSONObject inputJson = JSONObject.parseObject(inputString);
        String reqString = inputJson.getString(FullPullConstants.FULLPULL_REQ_PARAM);
        JSONObject reqJson = JSONObject.parseObject(reqString);
        String dataType = reqJson.getString("type");

        if (null == dataType) {
            logger.error("the type of request is null on PagedBatchDataFetchingBolt!");
            this.collector.fail(input);
            return;
        }

        try {
            if (dataType.equals(FullPullConstants.COMMAND_FULL_PULL_RELOAD_CONF)) {
                logger.info("[pull bolt] receive config reloading request :{}", inputJson);
                destory();
                loadRunningConf(reqString);
                this.collector.emit(FullPullConstants.CTRL_STREAM, new Values(inputJson));
                logger.info("[pull bolt] config for pull bolt is reloaded successfully!");
                return;
            } else if (dataType.equals(FullPullConstants.COMMAND_FULL_PULL_FINISH_REQ)) {
                // 写完成ok文件
                writeOkFile(reqString, reqJson);
                // 关闭hdfs资源
                HdfsConnectInfo hdfsConnectInfo = FullPullHelper.getHdfsConnectInfo(reqString);
                if (hdfsConnectInfo != null) {
                    if (hdfsConnectInfo.getFsDataOutputStream() != null) {
                        hdfsConnectInfo.getFsDataOutputStream().close();
                        logger.info("[pull bolt] close FSDataOutputStream ,filePath {}.", hdfsConnectInfo.getFilePath());
                    }
                    hdfsConnectInfo.setFsDataOutputStream(null);
                }
                FullPullHelper.setHdfsConnectInfo(reqString, hdfsConnectInfo);
                logger.info("[pull bolt] handle full pull finish command success!");
                return;
            } else {
                Long splitIndex = null;
                String dsKey = null;
                try {
                    if (FullPullHelper.hasErrorMessage(this.zkService, reqString)) {
                        collector.fail(input);
                        return;
                    }
                    dsKey = FullPullHelper.getDataSourceKey(reqJson);
                    InputSplit inputSplit = JSON.parseObject(inputJson.getString(FullPullConstants.DATA_CHUNK_SPLIT), InputSplit.class);
                    splitIndex = inputJson.getLong(FullPullConstants.DATA_CHUNK_SPLIT_INDEX);

                    Long startTime = System.currentTimeMillis();
                    logger.info("[pull bolt] {}, Fetching data for splitIndex {} start {}", dsKey, splitIndex, startTime);
                    DBConfiguration dbConf = FullPullHelper.getDbConfiguration(reqString);
                    String dsType = dbConf.getString(DBConfiguration.DataSourceInfo.DS_TYPE);
                    PullHandler pullHandler = FullPullHelper.getPullHandler(dsType);
                    pullHandler.setCollector(collector);
                    pullHandler.setKafkaSendBatchSize(kafkaSendBatchSize);
                    pullHandler.setKafkaSendRows(kafkaSendRows);
                    pullHandler.setHdfsSendBatchSize(hdfsSendBatchSize);
                    pullHandler.setHdfsFileMaxSize(hdfsFileMaxSize);
                    pullHandler.setStringProducer(stringProducer);
                    setHdfsParams(dbConf, pullHandler);
                    pullHandler.doPullingProcess(input, reqString, inputSplit, splitIndex);
                    logger.info("[pull bolt] {}, Fetching data for splitIndex {} end , cost time {}", dsKey, splitIndex, TimeUtils.formatTime(System.currentTimeMillis() - startTime));
                } catch (Exception e) {
                    String errorMsg = String.format("pull error! dsKey: %s, splitIndex: %s, %s", dsKey, splitIndex, e.getMessage());
                    if (dataType.equals(FullPullConstants.DATA_EVENT_INDEPENDENT_FULL_PULL_REQ)) {
                        FullPullHelper.finishPullReport(reqString, null, errorMsg);
                        this.collector.fail(input);
                    }
                    throw e;
                }
            }
        } catch (Exception e) {
            logger.error("Exception happended on batch data fetch bolt execute()", e);
        }
    }

    private void writeOkFile(String reqString, JSONObject reqJson) throws Exception {
        DBConfiguration dbConf = FullPullHelper.getDbConfiguration(reqString);
        String sinkType = dbConf.getString(DBConfiguration.SINK_TYPE);
        if (FullPullConstants.SINK_TYPE_KAFKA.equals(sinkType)) {
            //kafka 无需写ok文件
            return;
        }

        FSDataOutputStream outputStream = null;
        try {
            // 防止多个线程并发写hdfs同一个文件
            if (context.getThisTaskIndex() != 0) {
                logger.info("[pull bolt] 任务index:{},忽略写ok文件请求,仅index为0的任务负责写ok文件.", context.getThisTaskId());
                return;
            }
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            String opTs = dbConf.getString(DBConfiguration.DATA_IMPORT_OP_TS);
            String dbType = dbConf.getString(DBConfiguration.DataSourceInfo.DS_TYPE);
            // 这里oracle的ums_ts到微秒2020-04-21 12:46:45.461281,需要去掉后三位
            if (dbType.equals("oracle")) {
                opTs = opTs.substring(0, opTs.length() - 3);
            }
            long time = sdf.parse(opTs).getTime();
            String path = dbConf.getString(DBConfiguration.HDFS_TABLE_PATH) + "ok_" + time;
            logger.info("[pull bolt] will write ok file {}", path);

            outputStream = getFileSystem().create(new Path(path));
            JSONObject data = new JSONObject();
            data.put("ums_ts_", opTs);
            data.put("id", FullPullHelper.getSeqNo(reqJson));
            data.put("end_time", new Date());
            outputStream.write(data.toJSONString().getBytes());
            outputStream.hsync();
            logger.info("[pull bolt] write ok file success.{},{}", path, data);
        } catch (Exception e) {
            logger.error("Exception when write ok file to hdfs");
            throw e;
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
    }

    private void setHdfsParams(DBConfiguration dbConf, PullHandler pullHandler) throws Exception {
        if (FullPullConstants.SINK_TYPE_HDFS.equals(dbConf.getString(DBConfiguration.SINK_TYPE))) {
            pullHandler.setFileSystem(getFileSystem());
        }
    }

    private FileSystem getFileSystem() throws Exception {
        if (fileSystem != null) {
            return fileSystem;
        }
        Configuration conf = new Configuration();
        String hdfsUrl = hdfsConfigProps.getProperty(FullPullConstants.HDFS_URL);
        String hdoopUserName = hdfsConfigProps.getProperty(FullPullConstants.HADOOP_USER_NAME);
        System.setProperty("HADOOP_USER_NAME", hdoopUserName);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        if (StringUtils.isNotBlank(hdfsUrl)) {
            conf.set("fs.default.name", hdfsUrl);
            fileSystem = FileSystem.get(conf);
            logger.info("get FileSystem by hdfs url:{}, name:{}", hdfsUrl, fileSystem.getUri());
        } else {
            String coreSite = hdfsConfigProps.getProperty(FullPullConstants.CORE_SITE);
            String hdfsSite = hdfsConfigProps.getProperty(FullPullConstants.HDFS_SITE);
            if (coreSite.startsWith("http")) {
                conf.addResource(new URL(coreSite));
            } else {
                conf.addResource(new Path(coreSite));
            }
            if (hdfsSite.startsWith("http")) {
                conf.addResource(new URL(hdfsSite));
            } else {
                conf.addResource(new Path(hdfsSite));
            }
            conf.setBoolean("fs.hdfs.impl.disable.cache", true);
            fileSystem = FileSystem.newInstance(conf);
            logger.info("get FileSystem by core site :{} ,hdfs site :{},name:{}", coreSite, hdfsSite, fileSystem.getUri());
        }
        return fileSystem;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("progressInfo"));
        declarer.declareStream(FullPullConstants.CTRL_STREAM, new Fields("progressInfo"));
    }

    private void loadRunningConf(String reloadMsgJson) throws Exception {
        String notifyEvtName = reloadMsgJson == null ? "loaded" : "reloaded";
        String loadResultMsg = null;
        try {
            this.confMap = FullPullHelper.loadConfProps(this.zkconnect, this.topologyId, this.dsName, this.zkTopoRoot, "PullBatchDataFetchBolt");
            this.stringProducer = (Producer) this.confMap.get(FullPullHelper.RUNNING_CONF_KEY_STRING_PRODUCER);
            this.zkService = (ZkService) this.confMap.get(FullPullHelper.RUNNING_CONF_KEY_ZK_SERVICE);
            this.stringProducerProps = (Properties) this.confMap.get(FullPullHelper.RUNNING_CONF_KEY_STRING_PRODUCER_PROPS);
            this.hdfsConfigProps = (Properties) this.confMap.get(FullPullHelper.RUNNING_CONF_KEY_HDFS_CONF_PROPS);

            String sendBatchSizeStr = this.stringProducerProps.getProperty(FullPullConstants.SEND_BATCH_SIZE);
            String sendRowsStr = this.stringProducerProps.getProperty(FullPullConstants.SEND_ROWS);
            if (StringUtils.isNotBlank(sendBatchSizeStr) && (Long.valueOf(sendBatchSizeStr) != this.kafkaSendBatchSize)) {
                this.kafkaSendBatchSize = Long.valueOf(sendBatchSizeStr);
            }
            if (StringUtils.isNotBlank(sendRowsStr) && (Long.valueOf(sendRowsStr) != this.kafkaSendRows)) {
                this.kafkaSendRows = Long.valueOf(sendRowsStr);
            }

            String hdfsBatchSizeStr = hdfsConfigProps.getProperty(FullPullConstants.SEND_BATCH_SIZE);
            if (StringUtils.isNotBlank(hdfsBatchSizeStr) && (Long.valueOf(hdfsBatchSizeStr) != this.hdfsSendBatchSize)) {
                this.hdfsSendBatchSize = Long.valueOf(hdfsBatchSizeStr);
            }
            String hdfsFileSizeStr = hdfsConfigProps.getProperty(FullPullConstants.HDFS_FILE_SIZE);
            if (StringUtils.isNotBlank(hdfsFileSizeStr) && (Long.valueOf(hdfsFileSizeStr) != this.hdfsFileMaxSize)) {
                this.hdfsFileMaxSize = Long.valueOf(hdfsFileMaxSize);
            }

            loadResultMsg = "Running Config is " + notifyEvtName + " successfully for PagedBatchDataFetchingBolt!";
            logger.info(loadResultMsg);

            //初始化脱敏插件配置信息
            PluginManagerProvider.initialize(new FullPullPluginLoader());
            logger.info("[pull bolt] encode plugins init success ");
        } catch (Exception e) {
            loadResultMsg = e.getMessage();
            logger.error(notifyEvtName + "ing running configuration encountered Exception!", loadResultMsg);
            throw e;
        } finally {
            if (reloadMsgJson != null) {
                FullPullHelper.saveReloadStatus(reloadMsgJson, "pulling-dataFetching-bolt", false, zkconnect);
                //reload脱敏类型
                PluginManagerProvider.reloadManager();
                logger.info("[pull bolt] reload success ");
            }
        }
    }

    private void destory() {
        if (stringProducer != null) {
            stringProducer.close();
            stringProducer = null;
        }
        logger.info("close stringProducer");
    }

}
