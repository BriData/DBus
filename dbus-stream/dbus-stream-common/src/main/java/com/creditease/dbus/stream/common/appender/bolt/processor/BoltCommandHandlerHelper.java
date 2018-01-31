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

package com.creditease.dbus.stream.common.appender.bolt.processor;

import avro.shaded.com.google.common.collect.Maps;
import com.creditease.dbus.commons.*;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.DataTable;
import com.creditease.dbus.stream.common.appender.bean.MetaVersion;
import com.creditease.dbus.stream.common.appender.cache.ThreadLocalCache;
import com.creditease.dbus.stream.common.appender.utils.DBFacadeManager;
import com.creditease.dbus.stream.common.appender.utils.Pair;
import com.creditease.dbus.stream.common.appender.utils.PairWrapper;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.creditease.dbus.stream.common.Constants.CacheNames;
import static com.creditease.dbus.stream.common.Constants.MessageBodyKey;

/**
 * Created by zhangyf on 16/7/1.
 */
public class BoltCommandHandlerHelper {

    private static Logger logger = LoggerFactory.getLogger(BoltCommandHandlerHelper.class);

    public static void changeDataTableStatus(String schema, String table, String status) {
        String key = Utils.buildDataTableCacheKey(schema, table);
        // 修改data table的status字段
        DataTable dataTable = ThreadLocalCache.get(CacheNames.DATA_TABLES, key);
        if (dataTable == null) {
            logger.error("Table [{}.{}] not found", schema, table);
            throw new IllegalArgumentException("table not found");
        }
        String ostatus = dataTable.getStatus();
        switch (status) {
            case DataTable.STATUS_ABORT:
                dataTable.abort();
                break;
            case DataTable.STATUS_OK:
                dataTable.ok();
                break;
            case DataTable.STATUS_WAITING:
                dataTable.waiting();
                break;
            default:
                throw new IllegalArgumentException("Unknown data table status -> " + status);
        }

        // 同步修改数据库
        DBFacadeManager.getDbFacade().updateTableStatus(dataTable.getId(), status);
        if (DataTable.STATUS_ABORT.equals(status)) {
            logger.warn("Table [{}.{}] was changed, status [{} -> {}]", schema, table, ostatus, status);
        } else {
            logger.info("Table [{}.{}] was changed, status [{} -> {}]", schema, table, ostatus, status);
        }
    }

    public static DbusMessage buildTerminationMessage(String dbschema, String table, int version) {
        DbusMessageBuilder builder = new DbusMessageBuilder();
        String namespace = builder.buildNameSpace(Utils.getDataSourceNamespace(), dbschema, table, version);
        DataTable tab = ThreadLocalCache.get(Constants.CacheNames.DATA_TABLES, Utils.buildDataTableCacheKey(dbschema, table));

        DbusMessage message = builder.build(DbusMessage.ProtocolType.DATA_INCREMENT_TERMINATION, namespace, tab.getBatchId())
                .appendPayload(new Object[]{DateTime.now().toString("yyyy-MM-dd HH:mm:ss.SSS")})
                .getMessage();

        return message;
    }

    public static <T extends Object> PairWrapper<String, Object> convertAvroRecord(GenericRecord record, Set<T> noorderKeys) {
        Schema schema = record.getSchema();
        List<Schema.Field> fields = schema.getFields();
        PairWrapper<String, Object> wrapper = new PairWrapper<>();

        for (Schema.Field field : fields) {
            String key = field.name();
            Object value = record.get(key);
            // 分离存储是否关心顺序的key-value
            if (noorderKeys.contains(field.name())) {
                //wrapper.addProperties(key, value);
                addPairWrapperProperties(wrapper, key, value);
            }
        }

        GenericRecord before = getFromRecord(MessageBodyKey.BEFORE, record);
        GenericRecord after = getFromRecord(MessageBodyKey.AFTER, record);

        Map<String, Object> beforeMap = convert2map(before);
        Map<String, Object> afterMap = convert2map(after);

        // 覆盖before
        mergeMap(beforeMap, afterMap);

        for (Map.Entry<String, Object> entry : beforeMap.entrySet()) {
            if (!entry.getKey().endsWith(MessageBodyKey.IS_MISSING_SUFFIX)) {
                if ((Boolean) beforeMap.get(entry.getKey() + MessageBodyKey.IS_MISSING_SUFFIX)) {
                    wrapper.addMissingField(entry.getKey());
                }
                //wrapper.addPair(new Pair<>(entry.getKey(), CharSequence.class.isInstance(entry.getValue()) ? entry.getValue().toString() : entry.getValue()));
                addPairWrapperValue(wrapper, entry.getKey(), entry.getValue());
            }
        }

        return wrapper;
    }

    public static <T extends Object> PairWrapper<String, Object> convertAvroRecordUseBeforeMap(GenericRecord record, Set<T> noorderKeys) {
        Schema schema = record.getSchema();
        List<Schema.Field> fields = schema.getFields();
        PairWrapper<String, Object> wrapper = new PairWrapper<>();

        for (Schema.Field field : fields) {
            String key = field.name();
            Object value = record.get(key);
            // 分离存储是否关心顺序的key-value
            if (noorderKeys.contains(field.name())) {
                //wrapper.addProperties(key, value);
                addPairWrapperProperties(wrapper, key, value);
            }
        }

        GenericRecord before = getFromRecord(MessageBodyKey.BEFORE, record);

        Map<String, Object> beforeMap = convert2map(before);

        for (Map.Entry<String, Object> entry : beforeMap.entrySet()) {
            if (!entry.getKey().endsWith(MessageBodyKey.IS_MISSING_SUFFIX)) {
                //wrapper.addPair(new Pair<>(entry.getKey(), CharSequence.class.isInstance(entry.getValue()) ? entry.getValue().toString() : entry.getValue()));
                addPairWrapperValue(wrapper, entry.getKey(), entry.getValue());
            } else if ((Boolean) entry.getValue()) {
                wrapper.addMissingField(entry.getKey());
            }
        }

        return wrapper;
    }

    private static <T> T getFromRecord(String key, GenericRecord record) {
        return (T) record.get(key);
    }

    private static void mergeMap(Map<String, Object> m0, Map<String, Object> m) {
        for (Map.Entry<String, Object> entry : m.entrySet()) {
            if (!entry.getKey().endsWith(MessageBodyKey.IS_MISSING_SUFFIX)) {
                if (!(Boolean) m.get(entry.getKey() + MessageBodyKey.IS_MISSING_SUFFIX)) {
                    m0.put(entry.getKey(), entry.getValue());
                    m0.put(entry.getKey() + MessageBodyKey.IS_MISSING_SUFFIX, false);
                }
            }
        }
    }

    private static Map<String, Object> convert2map(GenericRecord record) {
        Map<String, Object> map = Maps.newHashMap();
        if (record != null) {
            List<Schema.Field> fields = record.getSchema().getFields();
            for (Schema.Field field : fields) {
                String key = field.name();
                map.put(key, record.get(key));
            }
        }
        return map;
    }

    public static void setMetaChangeFlag(String schema, String tableName) {
        setMetaChangeFlag(schema, tableName, DataTable.META_FLAG_CHANGED);
        logger.info("Table[{}.{}] meta change flag was set.", schema, tableName);
    }

    public static void clearMetaChangeFlag(String schema, String tableName) {
        setMetaChangeFlag(schema, tableName, DataTable.META_FLAG_DEFAULT);
        logger.info("Table[{}.{}] meta change flag was clean.", schema, tableName);
    }

    private static void setMetaChangeFlag(String schema, String tableName, int flag) {
        // 修改缓存
        DataTable table = ThreadLocalCache.get(Constants.CacheNames.DATA_TABLES, Joiner.on(".").join(schema, tableName));

        if (table == null) {
            logger.warn("No table found by schema:{},table:{}", schema, tableName);
            return;
        }
        // 修改数据库字段值
        DBFacadeManager.getDbFacade().updateMetaChangeFlag(table.getId(), flag);
        table.setMetaChangeFlg(flag);
    }

    public static void onBuildMessageError(String errId, MetaVersion version, Exception e) {
        Producer<String, String> producer = null;
        try {
            // 修改表的状态为：ABORT
            BoltCommandHandlerHelper.changeDataTableStatus(version.getSchema(), version.getTable(), DataTable.STATUS_ABORT);
            producer = createProducer();
            // 发送control message 通知appender所有线程reload，同步状态
            ControlMessage message = new ControlMessage(System.currentTimeMillis(), ControlType.APPENDER_RELOAD_CONFIG.name(), BoltCommandHandlerHelper.class.getName());
            ProducerRecord<String, String> record = new ProducerRecord<>(Utils.getDatasource().getControlTopic(), message.getType(), message.toJSONString());
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Send control message error.{}", message.toJSONString(), exception);
                }
            });

            // 发邮件
            ControlMessage gm = new ControlMessage(System.currentTimeMillis(), ControlType.COMMON_EMAIL_MESSAGE.toString(), BoltCommandHandlerHelper.class.getName());

            gm.addPayload("subject", "DBus生成消息异常报警");
            gm.addPayload("contents", String.format("[%s]dbus-stream 生成dbus message失败：%s/%s/%s，原因：%s", errId, Utils.getDatasource().getDsName(), version.getSchema(), version.getTable(), e.getMessage()));
            gm.addPayload("datasource_schema", Utils.getDatasource().getDsName() + "/" + version.getSchema());

            String topic = PropertiesHolder.getProperties(Constants.Properties.CONFIGURE, Constants.ConfigureKey.GLOBAL_EVENT_TOPIC);
            record = new ProducerRecord<>(topic, gm.getType(), gm.toJSONString());
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Send global event error.{}", exception.getMessage());
                }
            });
        } catch (Exception e1) {
            logger.error("exception data process error.", e1);
        } finally {
            if (producer != null) producer.close();
        }
    }


    public static void addPairWrapperProperties(PairWrapper<String, Object> pw, String key, Object value) {
        if (value instanceof GenericData.Array) {
            GenericData.Array array = (GenericData.Array) value;
            List<Object> list = Lists.newArrayList();
            for (Object v : array) {
                list.add(CharSequence.class.isInstance(v) ? v.toString() : v);
            }
            pw.addProperties(key, list);
        } else {
            pw.addProperties(key, CharSequence.class.isInstance(value) ? value.toString() : value);
        }
    }

    public static void addPairWrapperValue(PairWrapper<String, Object> pw, String key, Object value) {
        if (value instanceof GenericData.Array) {
            GenericData.Array array = (GenericData.Array) value;
            List<Object> list = Lists.newArrayList();
            for (Object v : array) {
                list.add(CharSequence.class.isInstance(v) ? v.toString() : v);
            }
            pw.addPair(new Pair<>(key, list));
        } else {
            pw.addPair(new Pair<>(key, CharSequence.class.isInstance(value) ? value.toString() : value));
        }
    }

    private static Producer<String, String> createProducer() throws Exception {
        Properties props = PropertiesHolder.getProperties(Constants.Properties.PRODUCER_CONFIG);
        props.setProperty("client.id", BoltCommandHandlerHelper.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);
        return producer;
    }
}
