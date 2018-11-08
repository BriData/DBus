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

package com.creditease.dbus.stream.common.tools;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.DBusConsumerRecord;
import com.creditease.dbus.commons.StatMessage;
import com.creditease.dbus.stream.common.DataSourceInfo;
import com.creditease.dbus.stream.common.bean.DispatcherPackage;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Future;

/**
 * Created by dongwang47 on 2016/8/18.
 */
public abstract class MessageProcessor {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected String statTopic;

    protected DataSourceInfo dsInfo;

    protected KafkaProducer<String, String> producer = null;

    public List<IGenericMessage> messageList  = new LinkedList<>();

    //stat info about count map, key is ds.schema.table, value is COUNT.
    protected TableStatMap statMap = null;

    protected Properties schemaTopicProps = null;

    //public List<OracleGenericMessage> messageList = null;

    public MessageProcessor(DataSourceInfo dsInfo, String statTopic, Properties producerProps, TableStatMap statMap, Properties schemaTopicProps) throws Exception {

        this.dsInfo = dsInfo;
        this.statTopic = statTopic;
        this.producer = new KafkaProducer<>(producerProps);
        logger.info("stat producer is ready.");

        this.statMap = statMap;
        this.schemaTopicProps = schemaTopicProps;
    }

    public void cleanup() throws Exception {
        if (producer != null) {
            producer.close();
            producer = null;
        }
    }

    public String getSchemaTopic(String schemaName) {

        if (schemaTopicProps != null) {
            String topicName = schemaTopicProps.getProperty(schemaName);
            if (topicName != null) {
                return topicName;
            }
        }

        //没有定义schema对应的topic，使用规则名称,schema就是topic名称
        return dsInfo.getDbSourceName() + "." + schemaName;
    }

    /**
     * template method getNextList
     * @return
     * @throws IOException
     */
    public List<DispatcherPackage> getNextList() throws IOException {

        String ctrlMessageKey = null;
        assert (messageList != null);
        Iterator<IGenericMessage> iter = messageList.iterator();
        if (!iter.hasNext()) {
            messageList = new LinkedList<>();
            return null;
        }

        // 1.根据schema 创建不同的hash map
        Map<String, List<IGenericMessage>> map = new HashMap<>();
        IGenericMessage msg;
        while(iter.hasNext()) {
            msg = iter.next();

            String schemaName = msg.getSchemaName();
            String tableName = msg.getTableName();
            int rowCount = msg.getRowCount();
            logger.debug(String.format("schemaName = %s, tableName = %s", schemaName, tableName));

            if (schemaName.equalsIgnoreCase(dsInfo.getDbusSchema())) {
                if (tableName.equalsIgnoreCase(Constants.FULL_PULL_TABLE)) {
                    if (map.size() != 0) {
                        logger.debug("这是中间的某一条是dbus数据，前面包含其他schema的数据,需要将前面的包先发送掉");
                        break;
                    }

                    if (processFullPullerMessage(map, msg) == 0) {
                        //正常情况
                            ctrlMessageKey = msg.getNameSpace() + "." + msg.getSchemaHash();
                        iter.remove();
                        break;
                    } else {
                        iter.remove(); //非insert的数据， 跳过了一条不需要传递的message
                        continue;
                    }

                } else if (tableName.equalsIgnoreCase(Constants.META_SYNC_EVENT_TABLE)) {
                    if (map.size() != 0) {
                        logger.debug("这是中间的某一条是dbus数据，前面包含其他schema的数据");
                        break;
                    }

                    if (processMetaSyncMessage(map, msg) == 0) {
                        //正常情况
                        ctrlMessageKey = msg.getNameSpace() + "." + msg.getSchemaHash();
                        iter.remove();
                        break;
                    }
                    else {
                        iter.remove(); //非insert的数据 ，跳过了一条不需要传递的message
                        continue;
                    }

                } else if (tableName.equalsIgnoreCase(Constants.HEARTBEAT_MONITOR_TABLE)) {
                    //根据SCHEMA_NAME发送对应的topic数据，不单独发
                    processHeartBeatMessage(map, msg);
                    iter.remove();
                    continue;
                }
            }

            //只有数据进行统计
            if (msg.isDML()) {
                statMark(schemaName, tableName, rowCount);
            }

            //按照schema来分包，因为schema对应不同topic
            List<IGenericMessage> subList = map.get(schemaName);
            if (subList != null) {
                subList.add(msg);
            } else  {
                subList = new LinkedList<>();
                subList.add(msg);
                map.put(schemaName, subList);
            }

            iter.remove(); //remove this message
        }


        // 2 将每个sublist 打包成为 byte[]
        List<DispatcherPackage> tuples = new LinkedList<>();
        for (HashMap.Entry<String, List<IGenericMessage>>  entry : map.entrySet()) {

            List<IGenericMessage> subList = entry.getValue();

            byte[] content = wrapMessages(subList);
            String schemaName = entry.getKey();
            int msgCount = subList.size();
            String toTopic = getSchemaTopic(schemaName);

            tuples.add(new DispatcherPackage(ctrlMessageKey, content, msgCount, schemaName, toTopic));
        }

        return tuples;
    }

    /**
     * template method, unwapMessage
     * @param record
     * @throws IOException
     */
    public void preProcess(DBusConsumerRecord<String, byte[]> record) throws IOException {

        messageList = unwrapMessages((byte[]) record.value());

        logger.info(String.format("Got Data : offset=%d, total_msg_count=%d, serializedValueSize=%d, from_topic: %s",
                record.offset(), messageList.size(), record.serializedValueSize(), record.topic()));
    }



    /***
     * send stat info to statistic topic, do not care about success or not.
     * @param message
     */
    private void sendTableStatInfo(StatMessage message) {

        String key = String.format("%s.%s.%s.%s.%s", message.getDsName(), message.getSchemaName(), message.getTableName(),
                message.getType(), message.getTxTimeMS());
        String value = message.toJSONString();

        Callback callback = new Callback() {
            @Override
            public void onCompletion(RecordMetadata ignored, Exception e) {
                if (e != null) {
                    logger.error(String.format("Send statistic FAIL: toTopic=%s, key=%s", statTopic, key));
                } else {
                    logger.info(String.format("  Send statistic successful: toTopic=%s, key=(%s)", statTopic, key));
                }
            }
        };

        Future<RecordMetadata> result = producer.send(new ProducerRecord<>(statTopic, key, value), callback);
    }

    //统计信息并插入到stat的kafka中
    public void statMeter(String schemaName, String tableName, long checkpointMS, long txTimeMS) {
        StatMessage message = statMap.logMeter(schemaName, tableName, checkpointMS, txTimeMS);
        sendTableStatInfo (message);
        message.cleanUp();
    }

    //统计计数
    public void statMark(String schemaName, String tableName, long count) {
        statMap.mark(schemaName, tableName, count);
    }



    public abstract List<IGenericMessage> unwrapMessages(byte[] data) throws IOException;

    public abstract byte[] wrapMessages(List<IGenericMessage> list) throws IOException;

    public abstract int processMetaSyncMessage(Map<String, List<IGenericMessage>> map, IGenericMessage obj) throws IOException;

    public abstract int processFullPullerMessage(Map<String, List<IGenericMessage>> map, IGenericMessage obj) throws IOException;

    public abstract int processHeartBeatMessage(Map<String, List<IGenericMessage>> map, IGenericMessage obj) throws IOException;
}
