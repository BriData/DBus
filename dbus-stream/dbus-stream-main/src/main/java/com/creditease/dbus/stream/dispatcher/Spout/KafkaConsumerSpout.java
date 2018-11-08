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

package com.creditease.dbus.stream.dispatcher.Spout;

import avro.shaded.com.google.common.collect.Lists;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.ControlType;
import com.creditease.dbus.commons.DBusConsumerRecord;
import com.creditease.dbus.commons.Pair;
import com.creditease.dbus.stream.common.DataSourceInfo;
import com.creditease.dbus.stream.dispatcher.helper.DBHelper;
import com.creditease.dbus.stream.dispatcher.helper.ZKHelper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaConsumerSpout extends BaseRichSpout {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    private KafkaConsumer<String, byte[]> consumer = null;

    private DataSourceInfo dsInfo = null;
    private TopicPartition dataTopicPartition = null;
    private TopicPartition ctrlTopicPartition = null;

    //用于 at lease once
    private long minFailedOffset = -1;
    private int minFailedCount = 0;

    private String zkServers = null;
    private String topologyID = null;
    private String topologyRoot;


    private Map conf = null;
    private SpoutOutputCollector collector = null;
    private int executingCount = 0;

    private int suppressLoggingCount = 0;
    private DBusConsumerRecord<String, byte[]> reloadRecord = null;

    /**
     *
     * @param reloadJson: reload control msg json
     */
    private void reloadConfig(String reloadJson) {

        close();

        ZKHelper zkHelper = null;
        DBHelper dbHelper = null;
        try {
            dsInfo = new DataSourceInfo();

            zkHelper = new ZKHelper(topologyRoot, topologyID, zkServers);
            zkHelper.loadDsNameAndOffset(dsInfo);

            dbHelper = new DBHelper(zkServers);
            dbHelper.loadDsInfo(dsInfo);
            logger.info(String.format("Spout read datasource: %s", dsInfo.toString()));

            //init consumer
            dataTopicPartition = new TopicPartition(dsInfo.getDataTopic(), 0);
            ctrlTopicPartition = new TopicPartition(dsInfo.getCtrlTopic(), 0);
            List<TopicPartition> topics = Arrays.asList(dataTopicPartition, ctrlTopicPartition);
            consumer = new KafkaConsumer(zkHelper.getConsumerProps());
            consumer.assign(topics);

            //skip offset
            long oldOffset = consumer.position(dataTopicPartition);
            logger.info(String.format("reloaded offset as: %d", oldOffset));

            String offset = dsInfo.getDataTopicOffset();
            if (offset.equalsIgnoreCase("none")) {
                ; // do nothing

            } else if (offset.equalsIgnoreCase("begin")) {
                consumer.seekToBeginning(Lists.newArrayList(dataTopicPartition));
                logger.info(String.format("Offset seek to begin, changed as: %d", consumer.position(dataTopicPartition)));

            } else if (offset.equalsIgnoreCase("end")) {
                consumer.seekToEnd(Lists.newArrayList(dataTopicPartition));
                logger.info(String.format("Offset seek to end, changed as: %d", consumer.position(dataTopicPartition)));
            } else {
                long nOffset = Long.parseLong(offset);
                consumer.seek(dataTopicPartition, nOffset);
                logger.info(String.format("Offset changed as: %d", consumer.position(dataTopicPartition)));
            }
            dsInfo.resetDataTopicOffset();

            zkHelper.saveDsInfo(dsInfo);
            zkHelper.saveReloadStatus(reloadJson, "dispatcher-spout", true);

        } catch (Exception ex) {
            logger.error("KafkaConsumerSpout reloadConfig():", ex);
            collector.reportError(ex);
            throw new RuntimeException(ex);
        } finally {
            if (dbHelper != null) {
                dbHelper.close();
            }
            if (zkHelper != null) {
                zkHelper.close();
            }
        }
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.conf = conf;
        this.collector = collector;

        //only init one time
        zkServers = (String) conf.get(Constants.ZOOKEEPER_SERVERS);
        topologyID = (String) conf.get(Constants.TOPOLOGY_ID);
        topologyRoot = Constants.TOPOLOGY_ROOT + "/" + topologyID;

        reloadConfig(null);
        logger.info("spout reload config success!");
    }

    @Override
    public void close () {
        if (consumer != null) {
            consumer.close();
            consumer = null;
        }
    }

    @Override
    public void activate() {

        logger.info("spout activate...");
    }

    @Override
    public void deactivate() {

        logger.info("spout deactivate...");
    }

    private void processControlCommand(DBusConsumerRecord<String, byte[]> record) {
        ControlType command = ControlType.getCommand(record.key());

        logger.info(String.format("Got Command : %s.  topic=%s, partition=%d, offset=%d, key=%s",
                command.name(), record.topic(), record.partition(), record.offset(), record.key()));

        switch (command) {
            case DISPATCHER_RELOAD_CONFIG:
                //save record for next time reload.
                reloadRecord = record;

                //commit this time
                commitOffset(new Pair<Long, String>(record.offset(), dsInfo.getCtrlTopic()));
                break;

            default:
                commitOffset(new Pair<Long, String>(record.offset(), dsInfo.getCtrlTopic()));
                break;
        }
    }

    /**
     * delay Print message
     * @return
     */
    private boolean canPrintNow () {
        suppressLoggingCount++;
        if (suppressLoggingCount % 10000 == 0) {
            suppressLoggingCount = 0;
            return true;
        }
        return false;
    }

    @Override
    public void nextTuple() {

        try {
            //流量控制, 防止内存溢出
            // 目前采用条来控制，其实应该用大小控制更好
            if (executingCount >= 30) {
                if (canPrintNow()) {
                    logger.warn(String.format("Flow Control: Spout executing %d records(offset=%d)!!!",
                            executingCount, consumer.position(dataTopicPartition)));
                }
                return;
            }

            //上次曾经受到过reload消息，因此进行reload
            if (reloadRecord != null) {
                logger.info(String.format("Before reload offset: %d", consumer.position(dataTopicPartition)));
                String json = new String(reloadRecord.value(), "utf-8");
                reloadConfig(json);
                //notify next bout to reload
                this.collector.emit(new Values(reloadRecord));
                reloadRecord = null;
            }

            long before = System.currentTimeMillis();
            ConsumerRecords<String, byte[]> records = consumer.poll(0);     // 快速取，如果没有就立刻返回
            if (records.count() == 0) {
                if (canPrintNow()) {
                    logger.info(String.format("Spout running.  executingCount = %d, offset=%d",
                            executingCount, consumer.position(dataTopicPartition)));
                }
                return;
            }
            long after = System.currentTimeMillis();
            logger.info(String.format("Spout got %d records......, poll() used_time: %d ms",  records.count(), after - before));

            for (ConsumerRecord<String, byte[]> record : records) {
                DBusConsumerRecord<String, byte[]> dbusRecord = new DBusConsumerRecord(record);
                if (dbusRecord.topic().equals(dsInfo.getCtrlTopic())) {
                    processControlCommand(dbusRecord);
                    continue;
                }

                executingCount++;
                logger.debug(String.format("Got Data: topic=%s, offset=%d, serializedValueSize=%d",  record.topic(), record.offset(), record.serializedValueSize()));
                this.collector.emit(new Values(dbusRecord), new Pair<Long, String>(record.offset(), dsInfo.getDataTopic()));
            }
        } catch (Exception ex) {
            logger.error("KafkaConsumerSpout nextTuple():", ex);
            collector.reportError(ex);
            throw new RuntimeException(ex);
        }
    }

    private void commitOffset(Pair<Long, String> topicOffset) {
        TopicPartition partition;
        if (topicOffset.getValue().equalsIgnoreCase(dsInfo.getCtrlTopic())) {
            partition = ctrlTopicPartition;
        } else {
            partition = dataTopicPartition;
        }
        OffsetAndMetadata offset = new OffsetAndMetadata(topicOffset.getKey() + 1, "");

        Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
        offsetMap.put(partition, offset);

        OffsetCommitCallback callback = new OffsetCommitCallback() {
            @Override
            public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                if (e != null) {
                    logger.warn(String.format("OK-CommitAsync failed!!!! offset %d, Topic %s", topicOffset.getKey(), topicOffset.getValue()));
                }
                else {
                    ; //do nothing when OK;
                    //logger.info(String.format("OK. offset %d, Topic %s", record.offset(), record.topic()));
                }
            }
        };

        consumer.commitAsync(offsetMap, callback);
    }

    private void seekOffsetBack(Pair<Long, String>  topicOffset) {
        TopicPartition partition;
        if (topicOffset.getValue().equalsIgnoreCase(dsInfo.getCtrlTopic())) {
            partition = ctrlTopicPartition;
        } else {
            partition = dataTopicPartition;
        }
        consumer.seek(partition, topicOffset.getKey());
    }

    @Override
    public void ack(Object msgId) {
        try{
            Pair<Long, String> topicOffset = (Pair<Long, String>)msgId;
            if (topicOffset.getValue().equalsIgnoreCase(dsInfo.getCtrlTopic())) {
                commitOffset(topicOffset);
            } else {
                //data topic
                executingCount--;

                long before = System.currentTimeMillis();
                if (minFailedOffset != -1) {
                    //有未处理的failed record
                    if (topicOffset.getKey() < minFailedOffset) {
                        commitOffset(topicOffset);
                        long after = System.currentTimeMillis();
                        logger.info(String.format("OK. Strange case. offset %d, Topic %s, used:%d ms",
                                topicOffset.getKey(), topicOffset.getValue(), after - before));
                    } else if (topicOffset.getKey() == minFailedOffset) {
                        minFailedOffset = -1;
                        minFailedCount = 0;
                        commitOffset(topicOffset);
                        long after = System.currentTimeMillis();
                        logger.info(String.format("OK. retry成功. 收到曾经fail过的offset %d的ack, Topic %s",
                                topicOffset.getKey(), topicOffset.getValue(), after - before));

                    } else {
                        logger.warn(String.format("OK. 正在等待ack的record是%d，收到跳过的ack record %d.",
                                minFailedOffset, topicOffset.getKey()));
                    }
                } else {
                    commitOffset(topicOffset);
                    long after = System.currentTimeMillis();
                    logger.debug(String.format("OK. offset %d, Topic %s, used: %d ms",
                            topicOffset.getKey(), topicOffset.getValue(), after - before));
                }
            }
        } catch (Exception ex) {
            logger.error("KafkaConsumerSpout ack():", ex);
            collector.reportError(ex);
            throw ex;
        }
    }



    /**
     * Failed should be abnormal case. I guess the reason inlcudes:
     *      1)  timeout
     *      2)  buffer overflow (or write kafka)
     *      3)  unknown reason
     * @param msgId
     */
    @Override
    public void fail(Object msgId) {
        try {
            Pair<Long, String> topicOffset = (Pair<Long, String>)msgId;
            if (topicOffset.getValue().equalsIgnoreCase(dsInfo.getCtrlTopic())) {
                commitOffset(topicOffset);
                return;
            } else {
                //data topic
                executingCount--;

                /* retry逻辑比较简单，就是保证at lease once, 即每个offset至少成功过一次。
                 * 在曾经失败过一次的前提下：
                 *      如果再失败，只记录最小的失败的offset，重做
                 *      如果成功，只能commit比最小失败offset以前的数据
                 */
                if (minFailedOffset != -1) {
                    if (topicOffset.getKey() < minFailedOffset) {
                        minFailedOffset = topicOffset.getKey();
                        minFailedCount = 1;
                        seekOffsetBack(topicOffset);
                        logger.warn(String.format("FAIL!!! offset %d, Topic %s, oldfailedoffset %d",
                                topicOffset.getKey(), topicOffset.getValue(), minFailedOffset));

                    } else if (topicOffset.getKey() == minFailedOffset) {
                        minFailedCount++;
                        seekOffsetBack(topicOffset);
                        logger.warn(String.format("FAIL!!! 第%d失败!! offset %d, Topic %s",
                                minFailedCount, topicOffset.getKey(), topicOffset.getValue()));
                    } else {
                        logger.warn(String.format("FAIL!!! 当前failedoffset 是%d, 收到比它大的offset %d",
                                minFailedOffset, topicOffset.getKey()));
                    }

                } else {
                    minFailedOffset = topicOffset.getKey();
                    minFailedCount = 1;
                    seekOffsetBack(topicOffset);
                    logger.warn(String.format("FAIL!!! offset %d, Topic %s", topicOffset.getKey(), topicOffset.getValue()));
                }
            }

        }catch (Exception ex) {
            logger.error("KafkaConsumerSpout fail():", ex);
            collector.reportError(ex);
            throw ex;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("record"));
    }

}
