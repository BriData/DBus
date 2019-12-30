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


package com.creditease.dbus.extractor.spout;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalPacket;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.DbusHelper;
import com.creditease.dbus.commons.Pair;
import com.creditease.dbus.extractor.common.utils.ZKHelper;
import com.creditease.dbus.extractor.container.ExtractorConfigContainer;
import com.creditease.dbus.extractor.container.MsgStatusContainer;
import com.creditease.dbus.extractor.container.TableMatchContainer;
import com.creditease.dbus.extractor.manager.ContainerMng;
import com.creditease.dbus.extractor.vo.MessageVo;
import com.creditease.dbus.extractor.vo.OutputTopicVo;
import com.creditease.dbus.extractor.vo.SendStatusVo;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by ximeiwang on 2017/8/15.
 */
public class CanalClientSpout extends BaseRichSpout {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    private boolean needReconnect = false;
    protected CanalConnector connector;

    private String zkConnectStr;
    protected String destination;

    private String zkServers;

    private String extractorName;
    private String extractorRoot;
    private final static int retryInterval = 10000;
    private long ackOrRollbackStartTime;
    private final static int ackOrRollbackInterval = 1000;
    private long checkSurvivalStartTime;
    private final static int checkSurvivalInterval = 20000;

    private Integer kafkaSendBatchSize = new Integer(1000);
    private int batchSize;
    private int flowSize;
    private String filter;
    private long timeout = 20;

    Integer printAlive = 0;//表明是否存活

    private Map conf = null;
    private SpoutOutputCollector collector = null;

    /**************************** reload读取kafka中消息 *****************************/
    private Consumer<String, byte[]> consumer = null;
    //private ConsumerRecord<String, byte[]> reloadRecord = null;
    //String extractorControlTopic = "extractorControlTopic";
    String extractorControlTopic;

    //对canal client 进行disconnect之后，进行处理
    protected int softStopCount = 0;

    protected boolean softStopProcess() {
        logger.info("starting soft stop process......");
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
        }
        softStopCount++;
        if (softStopCount >= 50 || MsgStatusContainer.getInstance().getSize() == 0) {
            softStopCount = 0;
            logger.info("soft stop process succeed.softStopCount: {}, MsgStatusContainer size is {} .",
                    softStopCount, MsgStatusContainer.getInstance().getSize());
            return true;
        }
        return false;
    }

    /********************************************************************************/
    private void reloadConfig(String reloadJson) {
        logger.info("spout: canal client reload starting......");
        ZKHelper zkHelper = null;
        try {
            ContainerMng.clearAllContainer();//清除所有container中存在的信息

            zkHelper = new ZKHelper(zkServers, extractorRoot, extractorName);
            zkHelper.loadJdbcConfig();
            zkHelper.loadExtractorConifg();
            zkHelper.loadOutputTopic();
            zkHelper.loadFilter();
            zkHelper.loadKafkaProducerConfig();
            zkHelper.loadKafkaConsumerConfig();
            logger.info("spout: 1 reload zk OK!");

            Integer kafkaBatchSize = ExtractorConfigContainer.getInstances().getExtractorConfig().getKafkaSendBatchSize();
            if (kafkaBatchSize != null)
                kafkaSendBatchSize = kafkaBatchSize;
            batchSize = ExtractorConfigContainer.getInstances().getExtractorConfig().getCanalBatchSize();
            flowSize = ExtractorConfigContainer.getInstances().getExtractorConfig().getCanalFlowSize();

            /****************************初始化控制reload的kafka consumer************************/
            if (consumer != null) {
                consumer.close();
                consumer = null;
            }
            //不需要for循环 20180307 review
            for (OutputTopicVo vo : ExtractorConfigContainer.getInstances().getOutputTopic()) {
                extractorControlTopic = vo.getControlTopic();
                break;
            }
            consumer = DbusHelper.createConsumer(ExtractorConfigContainer.getInstances().getKafkaConsumerConfig(),
                    extractorControlTopic);
            logger.info("spout: 2 reload kafka consumer OK!");
            /***********************************************************************************/

            /***判断连接canal connect的zkconnectstr和destination是否改变，如果改变则断开重新连接，否则只进行后续订阅即可***/
            String canalZkPath = ExtractorConfigContainer.getInstances().getExtractorConfig().getCanalZkPath();
            String newZkConnectStr = zkServers + canalZkPath;
            String newDestination = ExtractorConfigContainer.getInstances().getExtractorConfig().getCanalInstanceName();
            if (connector == null || (connector != null && connector.checkValid() == false)) {
                //连接不可用
                if (connector != null && connector.checkValid() == false) {
                    //有连接，连接不可用
                    logger.error("spout: connect is not valid!");
                    connector.disconnect();
                    if (softStopProcess()) {
                        MsgStatusContainer.getInstance().clear();
                    }
                }
                assert (zkConnectStr == null && destination == null);
                zkConnectStr = newZkConnectStr;
                destination = newDestination;
                connector = CanalConnectors.newClusterConnector(newZkConnectStr, newDestination, "", "");
                connector.connect();
                logger.info("spout: 3 canal reload OK!");
            } else {
                //连接是可以用的
                if (!newZkConnectStr.equals(zkConnectStr) || !newDestination.equals(destination)) {
                    //配置发生变化
                    logger.info("spout: canal reconnected, zk config have been changed!");
                    connector.disconnect();
                    if (softStopProcess()) {
                        MsgStatusContainer.getInstance().clear();
                    }
                    connector = CanalConnectors.newClusterConnector(newZkConnectStr, newDestination, "", "");
                    connector.connect();
                    zkConnectStr = newZkConnectStr;
                    destination = newDestination;
                    logger.info("spout: 3 canal reload OK!");
                } else {
                    logger.info("spout: 3 canal reload OK! zk config not changed!");
                }
            }

            filter = ExtractorConfigContainer.getInstances().getFilter();
            connector.subscribe(filter);
            logger.info("spout: canal subscribe filter is: {}", filter);

            //写reload状态到zookeeper
            zkHelper.saveReloadStatus(reloadJson, "extractor-canal-client-spout", true);

            logger.info("spout: reload all_OK!");

        } catch (Exception ex) {
            logger.error("spout reloadConfig()", ex);
            collector.reportError(ex);
            //throw new RuntimeException(ex);
        } finally {
            if (zkHelper != null) {
                zkHelper.close();
            }
        }
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.conf = conf;
        this.collector = collector;
        this.zkServers = (String) conf.get(Constants.ZOOKEEPER_SERVERS);
        this.extractorName = (String) conf.get(Constants.EXTRACTOR_TOPOLOGY_ID);
        this.extractorRoot = Constants.EXTRACTOR_ROOT + "/";

        reloadConfig(null);
        ackOrRollbackStartTime = System.currentTimeMillis();
        checkSurvivalStartTime = System.currentTimeMillis();

        logger.info("CanalClientSpout() open finished");
    }

    /****流量控制处理函数****/
    private boolean flowLimitation() {
        int hasSentBatchSize = MsgStatusContainer.getInstance().getSize();
        if (hasSentBatchSize >= flowSize) {
            logger.info("Flow control: Spout gets {} bytes data.", hasSentBatchSize);
            try {
                ackOrRollback();
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
            return true;
        }
        return false;
    }

    @Override
    public void nextTuple() {
        try {
            //  上次如果出现退出错误
            if (needReconnect) {
                logger.info("Spout canal reconnect needed ....");
                connector.connect();
                filter = ExtractorConfigContainer.getInstances().getFilter();
                connector.subscribe(filter);
                logger.info("Reconnect success!  canal client subscribe the filter is {}", filter);
                needReconnect = false;
            }

            if (flowLimitation()) {
                return; // 如果读取的流量过大则要sleep一下
            }

            //优先处理控制消息
            ConsumerRecords<String, byte[]> records = consumer.poll(0);
            for (ConsumerRecord<String, byte[]> record : records) {
                String key = record.key();
                if (key.equals("EXTRACTOR_RELOAD_CONF")) {
                    //收到到过reload消息，因此进行reload
                    String event = new String(record.value(), "utf-8");
                    logger.info("Received a EXTRACTOR_RELOAD_CONF Message {key:{}, event:{}}", key, event);
                    reloadConfig(event);
                    //notify next bout to reload
                    this.collector.emit(new Values("message", record));
                    return;
                } else {
                    logger.info("Skipped unknown command key:{}", key);
                }
            }


// 探活需要是否？？
//            long curTime = System.currentTimeMillis();
//            if (curTime - checkSurvivalStartTime >= checkSurvivalInterval) {
//                checkSurvivalStartTime = curTime;
//                if(connector.checkValid() == false){
//                    logger.error("canal connect is not valid, will reconnect it......");
//                    connector.disconnect();
//                    if(softStopProcess()) {
//                        MsgStatusContainer.getInstance().clear();
//                    }
//                    connector.connect();
//                    if (connector.checkValid()){
//                        logger.info("canal client connect succeed.");
//                    }
//                    filter = ExtractorConfigContainer.getInstances().getExtractorConfig().getSubscribeFilter();
//                    connector.subscribe(filter);
//                    logger.info("canal client subscribe success, the filter is {}", filter);
//                }
//            }

            //如果没有消息,处理canal抓取数据
            Message message = connector.getWithoutAck(batchSize, timeout, TimeUnit.MILLISECONDS);
            long batchId = message.getId();
            int size = message.getEntries().size();

            if (hasData(batchId, size)) {
                //logger.info("canal has get data, batch id is: {}, the batch size is: {}, message entries is {}.",
                //        batchId, size, message.getEntries());
                produceData(message);
            } else {
                if (printAlive == 0) {
                    logger.info("canal spout is alive.");
                }
                printAlive = printAlive + 1;
                if (printAlive >= 500) {
                    printAlive = 0;
                }
            }

            //设定一个时间间隔
            long curTime = System.currentTimeMillis();
            if (curTime - ackOrRollbackStartTime >= ackOrRollbackInterval) {
                ackOrRollback();
                ackOrRollbackStartTime = curTime;
            }

        } catch (CanalClientException e) {
            needReconnect = true;
            connector.disconnect();
            logger.warn("CanalClientException error", e);
            try {
                Thread.sleep(retryInterval);
            } catch (InterruptedException e1) {
                logger.warn("sleep error", e1);
            }
        } catch (Exception e) {
            logger.error("process error!", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message", "reloadControl"));
    }

    @Override
    public void close() {
        if (connector != null) {
            connector.disconnect();
            connector = null;
        }
    }

    @Override
    public void activate() {

    }

    @Override
    public void ack(Object msgId) {
        try {
            Pair<Long, String> pair = (Pair<Long, String>) msgId;
            logger.info("spout ack batchId: " + pair.getKey());
            MsgStatusContainer.getInstance().setCompleted(pair.getKey(), 1);
            //设定一个时间间隔
            long curTime = System.currentTimeMillis();
            if (curTime - ackOrRollbackStartTime >= ackOrRollbackInterval) {
                ackOrRollback();
                ackOrRollbackStartTime = curTime;
            }
        } catch (Exception e) {
            logger.error("spout ack exception {}", e);
        }
    }

    @Override
    public void fail(Object msgId) {
        try {
            Pair<Long, String> pair = (Pair<Long, String>) msgId;
            logger.error("spout fail batchId: " + pair.getKey());
            MsgStatusContainer.getInstance().setError(pair.getKey(), true);
            ackOrRollbackStartTime = System.currentTimeMillis();
            ackOrRollback();
        } catch (Exception e) {
            logger.error("spout fail exception {}", e);
        }

    }

    private boolean hasData(long batchId, int msgSize) {
        if (batchId == -1 || msgSize == 0)
            return false;
        return true;
    }

    private void produceData(Message msg) {
        logger.debug("starting produce message data......");
        int serializedSize = 0;
        int batchCount = 0;
        Integer split = 0;
        CanalPacket.Messages.Builder builder = CanalPacket.Messages.newBuilder();
        builder.setBatchId(msg.getId());
        for (CanalEntry.Entry entry : msg.getEntries()) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                logger.debug("the entry type is transaction begin or transaction end.");
                continue;
            }
            //处理分区表
            logger.debug("the entry type is row data.");
            String tableName = entry.getHeader().getTableName();
            String localTable = TableMatchContainer.getInstance().getLocalTable(tableName);
            if (!tableName.equals(localTable)) {
                String finalTable = StringUtils.join(new String[]{localTable, tableName}, ".");
                CanalEntry.Header header = CanalEntry.Header.newBuilder(entry.getHeader()).setTableName(finalTable).build();
                entry = CanalEntry.Entry.newBuilder(entry).setHeader(header).build();
            }

            // 如果加上当前这条，大小已经超过最大数，就发送
            if (serializedSize + entry.getSerializedSize() >= kafkaSendBatchSize) {
                MsgStatusContainer.getInstance().setTotal(msg.getId(), ++split, false);
                MessageVo msgVo = new MessageVo();
                msgVo.setBatchId(msg.getId());
                msgVo.setMessage(builder.build().toByteArray());

                logger.info("produceData serializedSize:{}, batchCount:{}", serializedSize, batchCount);
                this.collector.emit(new Values(msgVo, "controlReCord"), new Pair<Long, Integer>(msg.getId(), split));
                logger.debug("message to bolt, the batch id is {}, and it's the {} fragment.", msg.getId(), split);

                serializedSize = 0;
                batchCount = 0;
                builder.clearMessages();
            }

            serializedSize += entry.getSerializedSize();
            batchCount += 1;
            builder.addMessages(entry.toByteString());
        }

        if (builder.getMessagesCount() > 0) {
            MsgStatusContainer.getInstance().setTotal(msg.getId(), ++split, true);
            logger.info("split produce just done, the batch id is {}, split is {}.", msg.getId(), split);
            MessageVo msgVo = new MessageVo();
            msgVo.setBatchId(msg.getId());
            msgVo.setMessage(builder.build().toByteArray());

            logger.info("produceData serializedSize:{}, batchCount:{}", serializedSize, batchCount);
            this.collector.emit(new Values(msgVo, "controlReCord"), new Pair<Long, Integer>(msg.getId(), split));
            builder.clear();
        } else {
            if (split != 0) {
                logger.info("split produce just done, the batch id is {}, split is {}.", msg.getId(), split);
                MsgStatusContainer.getInstance().setTotal(msg.getId(), split, true);
            } else {
                logger.info("Empty batch id is {}. No data to kafka", msg.getId());
                MsgStatusContainer.getInstance().setTotal(msg.getId(), 1, true);
                MsgStatusContainer.getInstance().setCompleted(msg.getId(), 1);
            }
        }
        builder = null;
        //logger.info("receive one message,the batchId:{},split:{}", msg.getId(), split);//todo
    }

    private void ackOrRollback() {
        Set<SendStatusVo> set = MsgStatusContainer.getInstance().getNeedAckOrRollbackBatch();
        Iterator<SendStatusVo> iter = set.iterator();
        while (iter.hasNext()) {
            SendStatusVo vo = iter.next();
            if (vo.getResult() == Constants.NEED_ACK_CANAL && vo.getBatchId() != 0) {
                connector.ack(vo.getBatchId());
                MsgStatusContainer.getInstance().deleteMsg(vo.getBatchId());
                int totalBatchSize = MsgStatusContainer.getInstance().getSize();
                logger.info("batchId {} ack OK! Unfinished batch size is {}", vo.getBatchId(), totalBatchSize);
            } else if (vo.getResult() == Constants.NEED_ROLLBACK_CANAL && vo.getBatchId() != 0) {
                //由于canal的batchId为自增变量，然后所有的消息必须依次ack/rollback，不能跳跃，因此针对某个具体的batchId
                //进行rollback，会出现rollback失败，具体可参考canal源码
                connector.rollback();
                // connector.rollback(vo.getBatchId());
                int totalBatchSize = MsgStatusContainer.getInstance().getSize();
                logger.warn("rollback canal, the batchId is {}, unfinished batch size is {}", vo.getBatchId(), totalBatchSize);
                MsgStatusContainer.getInstance().clear();
                logger.warn("batch size cleared, unfinished batch size is {}", totalBatchSize);
                break;
            } else if (vo.getResult() == Constants.SEND_NOT_COMPLETED) {
                break;
            }
        }
        set = null;
    }

}

