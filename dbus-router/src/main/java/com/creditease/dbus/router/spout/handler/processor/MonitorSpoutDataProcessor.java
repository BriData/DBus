package com.creditease.dbus.router.spout.handler.processor;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.router.bean.Packet;
import com.creditease.dbus.router.spout.aware.ContextAware;
import com.creditease.dbus.router.spout.aware.KafkaConsumerAware;
import com.creditease.dbus.router.spout.context.Context;
import com.creditease.dbus.router.spout.context.ProcessorContext;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitorSpoutDataProcessor extends AbstractProcessor implements ContextAware, KafkaConsumerAware {

    private static Logger logger = LoggerFactory.getLogger(MonitorSpoutDataProcessor.class);
    private Long baseFlushTime = 0L;
    private Map<String, Packet> cache = new HashMap<>();
    private ProcessorContext context = null;
    private KafkaConsumer consumer = null;

    @Override
    public void setContext(Context context) {
        this.context = (ProcessorContext) context;
    }

    @Override
    public void setKafkaConsumer(KafkaConsumer kafkaConsumer) {
        consumer = kafkaConsumer;
    }

    @Override
    public boolean isBelong(String str) {
        if (StringUtils.isBlank(str))
            return false;
        String[] vals = StringUtils.split(str, ".");
        return StringUtils.equals("data_increment_heartbeat", vals[0]);
    }

    private void ack(TopicPartition topicPartition, long offset) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(topicPartition, new OffsetAndMetadata(offset));
        consumer.commitSync(offsets);
        logger.info("consumer commitSync topic:{}, offset:{}", topicPartition.toString(), offset);
    }

    private void fail(TopicPartition topicPartition, long offset) {
        consumer.seek(topicPartition, offset);
        logger.info("consumer seek topic:{}, offset:{}", topicPartition.toString(), offset);
    }

    @Override
    public Object process(Object obj, Supplier ... suppliers) {
        Object ret = new Object();

        ConsumerRecord<String, byte[]> record = (ConsumerRecord) obj;
        logger.info("topic:{}, key:{}, offset:{}", record.topic(), record.key(), record.offset());

        if (!isBelong(record.key()))
            return ret;

        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
        try {
            if (StringUtils.isEmpty(record.key())) {
                logger.warn("topic:{}, offset:{}, key is empty", record.topic(), record.offset());
                return ret;
            }

            // data_increment_heartbeat.mysql.mydb.cbm.t1#router_test_s_r5.6.0.0.1541041451552|1541041451550|ok.wh_placeholder
            String[] vals = StringUtils.split(record.key(), ".");
            if (vals == null || vals.length != 10) {
                logger.error("receive heartbeat key is error. topic:{}, offset:{}, key:{}", record.topic(), record.offset(), record.key());
                return ret;
            }

            long cpTime = 0L;
            long txTime = 0L;
            boolean isTableOK = true;
            String originDsName = StringUtils.EMPTY;

            if (StringUtils.contains(vals[8], "|")) {
                String times[] = StringUtils.split(vals[8], "|");
                cpTime = Long.valueOf(times[0]);
                txTime = Long.valueOf(times[1]);
                // 表明其实表已经abort了，但心跳数据仍然, 这种情况，只发送stat，不更新zk
                if ((times.length == 3 || times.length == 4) && times[2].equals("abort")) {
                    isTableOK = false;
                    logger.warn("data abort. key:{}", record.key());
                }
                if (times.length == 4)
                    originDsName = times[3];
            } else {
                isTableOK = false;
                logger.error("it should not be here. key:{}", record.key());
            }

            if (!isTableOK)
                return ret;

            String dsName = vals[2];
            if (StringUtils.contains(vals[2], "!")) {
                dsName = StringUtils.split(vals[2], "!")[0];
            } else {
                isTableOK = false;
                logger.error("it should not be here. key:{}", record.key());
            }

            if (StringUtils.isNoneBlank(originDsName))
                dsName = originDsName;

            String schemaName = vals[3];
            String tableName = vals[4];

            if (!isTableOK)
                return ret;

            // String dsPartition = vals[6];
            String ns = StringUtils.joinWith(".", dsName, schemaName, tableName);
            String path = StringUtils.joinWith("/", Constants.HEARTBEAT_PROJECT_MONITOR,
                    context.getInner().projectName, context.getInner().topologyId, ns);

            // {"node":"/DBus/HeartBeat/ProjectMonitor/db4new/AMQUE/T_USER/0","time":1531180006336,"type":"checkpoint","txTime":1531180004040}
            Packet packet = new Packet();
            packet.setNode(path);
            packet.setType("checkpoint");
            packet.setTime(cpTime);
            packet.setTxTime(txTime);
            cache.put(path, packet);
            logger.info("put cache path:{}", path);

            if (isTimeUp(baseFlushTime)) {
                baseFlushTime = System.currentTimeMillis();
                logger.info("router update zk stat :{}", baseFlushTime);
                flushCache();
            }
            ack(topicPartition, record.offset());
        } catch (Exception e) {
            logger.error("consumer record processor process fail.", e);
            fail(topicPartition, record.offset());
        }
        return ret;
    }

    private boolean isTimeUp(long baseTime) {
        return (System.currentTimeMillis() - baseTime) > (1000 * 60);
    }

    private void flushCache() {
        Iterator<String> it = cache.keySet().iterator();
        while (it.hasNext()) {
            String key = it.next();
            serialize(key, cache.get(key));
        }
        cache.clear();
    }

    private void serialize(String path, Packet packet) {
        if (packet == null)
            return;
        String strJson = JSONObject.toJSONString(packet);
        byte[] data = strJson.getBytes(Charset.forName("UTF-8"));
        logger.info("serialize zk data path:{}, value:{}", path, strJson);
        context.getInner().zkHelper.setData(path, data);
    }

}
