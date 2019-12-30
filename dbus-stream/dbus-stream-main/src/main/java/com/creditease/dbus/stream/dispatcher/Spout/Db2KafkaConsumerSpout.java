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


package com.creditease.dbus.stream.dispatcher.Spout;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.ControlType;
import com.creditease.dbus.commons.DBusConsumerRecord;
import com.creditease.dbus.stream.appender.spout.queue.MessageStatusQueueManager;
import com.creditease.dbus.stream.common.DataSourceInfo;
import com.creditease.dbus.stream.common.appender.kafka.TopicProvider;
import com.creditease.dbus.stream.dispatcher.helper.DBHelper;
import com.creditease.dbus.stream.dispatcher.helper.ZKHelper;
import com.creditease.dbus.stream.dispatcher.kafka.Db2InputTopicProvider;
import com.creditease.dbus.stream.dispatcher.tools.Db2OffsetReset;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Db2KafkaConsumerSpout extends BaseRichSpout {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    private KafkaConsumer<byte[], byte[]> db2Consumer = null;
    private KafkaConsumer<String, byte[]> ctrlConsumer = null;

    private DataSourceInfo dsInfo = null;

    private TopicPartition ctrlTopicPartition = null;

    //db2订阅多个topic，key:topicName, value: 对应的topicPartition
    private Map<String, TopicPartition> topicPartitionMap = new HashMap<>();


    private String zkServers = null;
    private String topologyID = null;
    private String topologyRoot;

    private MessageStatusQueueManager msgQueueMgr;


    private Map conf = null;
    private SpoutOutputCollector collector = null;

    private int suppressLoggingCount = 0;
    private DBusConsumerRecord<String, byte[]> reloadRecord = null;

    /**
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


            this.msgQueueMgr = new MessageStatusQueueManager();

            TopicProvider topicProvider;
            List<TopicPartition> topicPartitions = new ArrayList<>();
            List<String> topics;

            if (StringUtils.equals(dsInfo.getDbSourceType(), Constants.DB2_CONFIG.DB2)) {
                topicProvider = new Db2InputTopicProvider(zkServers, dsInfo);
                topics = topicProvider.provideTopics();

                for (String topic : topics) {
                    TopicPartition tp = new TopicPartition(topic, 0);
                    topicPartitions.add(tp);
                    topicPartitionMap.put(topic, tp);
                }
            }

            db2Consumer = new KafkaConsumer(zkHelper.getDb2ConsumerProps());
            db2Consumer.assign(topicPartitions);

            ctrlConsumer = new KafkaConsumer(zkHelper.getConsumerProps());
            List<TopicPartition> ctrlTopicPartitions = new ArrayList<>();
            ctrlTopicPartitions.add(new TopicPartition(dsInfo.getCtrlTopic(), 0));
            ctrlConsumer.assign(ctrlTopicPartitions);

            //重设读取数据的offset点，保证可重复读数据
            if (StringUtils.equals(dsInfo.getDbSourceType(), Constants.DB2_CONFIG.DB2)) {
                Db2OffsetReset db2OffsetReset = new Db2OffsetReset(db2Consumer, dsInfo);
                db2OffsetReset.offsetReset(topicPartitions);
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
    public void close() {
        if (db2Consumer != null) {
            db2Consumer.close();
            db2Consumer = null;
        }
        if (ctrlConsumer != null) {
            ctrlConsumer.close();
            ctrlConsumer = null;
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
                break;
            default:
                break;
        }
    }

    /**
     * delay Print message
     *
     * @return
     */
    private boolean canPrintNow() {
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
            //上次曾经收到过reload消息，因此进行reload
            if (reloadRecord != null) {
                String json = new String(reloadRecord.value(), "utf-8");
                reloadConfig(json);
                //notify next bout to reload
                this.collector.emit(new Values(reloadRecord, Constants.DB2_CONFIG.EMIT_CTRL_TYPE));
                reloadRecord = null;
            }

            long before = System.currentTimeMillis();

            ConsumerRecords<String, byte[]> ctrlRecords = ctrlConsumer.poll(0);
            ConsumerRecords<byte[], byte[]> dataRecords = db2Consumer.poll(0);     // 快速取，如果没有就立刻返回

            if (dataRecords.count() == 0 && ctrlRecords.count() == 0) {
                if (canPrintNow()) {
                    logger.info("Spout running.");
                }
                return;
            }

            long after = System.currentTimeMillis();

            if (dataRecords.count() != 0) {
                logger.info(String.format("Spout got %d data records......, poll() used_time: %d ms", dataRecords.count(), after - before));

                List<DBusConsumerRecord<String, byte[]>> list = new ArrayList<>();
                for (ConsumerRecord<byte[], byte[]> record : dataRecords) {
                    DBusConsumerRecord<String, byte[]> dbusRecord = new DBusConsumerRecord(record.topic(), record.partition(), record.offset(), record.timestamp(), record.timestampType(),
                            record.checksum(), record.serializedKeySize(), record.serializedKeySize(), record.key(), record.key());

                    logger.info(String.format("Got Data: topic=%s, offset=%d, serializedValueSize=%d", record.topic(), record.offset(), record.serializedValueSize()));

                    msgQueueMgr.addMessage(dbusRecord);
                    list.add(dbusRecord);
                }

                this.collector.emit(new Values(list, Constants.DB2_CONFIG.EMIT_DATA_TYPE), list);
            }

            if (ctrlRecords.count() != 0) {
                logger.info(String.format("Spout got %d ctrl records......, poll() used_time: %d ms", ctrlRecords.count(), after - before));

                for (ConsumerRecord<String, byte[]> record : ctrlRecords) {
                    DBusConsumerRecord<String, byte[]> dbusRecord = new DBusConsumerRecord(record);
                    if (dbusRecord.topic().equals(dsInfo.getCtrlTopic())) {
                        processControlCommand(dbusRecord);
                        continue;
                    }
                    logger.info(String.format("Got Data: topic=%s, offset=%d, serializedValueSize=%d", record.topic(), record.offset(), record.serializedValueSize()));
                }
            }

        } catch (Exception ex) {
            logger.error("KafkaConsumerSpout nextTuple():", ex);
            collector.reportError(ex);
            throw new RuntimeException(ex);
        }
    }


    @Override
    public void ack(Object msgId) {
        if (msgId != null && ArrayList.class.isInstance(msgId)) {
            List<DBusConsumerRecord<String, byte[]>> records = getMessageId(msgId);
            for (DBusConsumerRecord<String, byte[]> record : records) {
                // 标记处理成功并获取commit点
                DBusConsumerRecord<String, byte[]> commitPoint = msgQueueMgr.okAndGetCommitPoint(record);
                if (commitPoint != null) {
                    syncOffset(commitPoint);
                    msgQueueMgr.committed(commitPoint);
                }
                logger.info("[dbus-spout-ack] topic: {}, offset: {}, size:{}, sync-kafka-offset:{}", record.topic(), record.offset(), record.serializedValueSize(), commitPoint != null ? commitPoint.offset() : -1);
            }
        } else {
            logger.info("[dbus-spout-ack] receive ack message[{}]", msgId.getClass().getName());
        }
        super.ack(msgId);
    }


    @Override
    public void fail(Object msgId) {

        if (msgId != null && ArrayList.class.isInstance(msgId)) {
            List<DBusConsumerRecord<String, byte[]>> records = getMessageId(msgId);
            for (DBusConsumerRecord<String, byte[]> record : records) {
                logger.error("[dbus-spout-fail] topic: {}, offset: {}, size: {}", record.topic(), record.offset(), record.serializedValueSize());
                // 标记处理失败,获取seek点
                DBusConsumerRecord<String, byte[]> seekPoint = msgQueueMgr.failAndGetSeekPoint(record);
                if (seekPoint != null) {
                    seek(seekPoint);
                }
            }

        } else {
            logger.info("[dbus-spout-fail] receive ack message[{}]", msgId.getClass().getName());
        }
        super.fail(msgId);
    }


    public void syncOffset(DBusConsumerRecord<String, byte[]> record) {
        logger.debug("Ack offset {topic:{},partition:{},offset:{}}", record.topic(), record.partition(), record.offset() + 1);
        final String topic = record.topic();
        final int partition = record.partition();
        final long offset = record.offset();
        Map<TopicPartition, OffsetAndMetadata> map = Collections.singletonMap(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1, ""));
        db2Consumer.commitAsync(map, (metadata, exception) -> {
            if (exception != null) {
                logger.error("[appender-db2Consumer] commit error [topic:{},partition:{},offset:{}]., error message: {}", topic, partition, offset, exception.getMessage());
            }
        });
    }

    public void seek(DBusConsumerRecord<String, byte[]> record) {
        logger.info("seek topic {topic:{},partition:{},offset:{}}", record.topic(), record.partition(), record.offset());
        db2Consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
    }


    private <T> T getMessageId(Object msgId) {
        return (T) msgId;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("record", "emitType"));
    }

}
