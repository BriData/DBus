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


package com.creditease.dbus.stream.mongo.appender.bolt.processor.wrapper;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.DataType;
import com.creditease.dbus.commons.DbusMessage;
import com.creditease.dbus.commons.DbusMessageBuilder;
import com.creditease.dbus.commons.PropertiesHolder;
import com.creditease.dbus.stream.common.Constants;
import com.creditease.dbus.stream.common.appender.bean.DataTable;
import com.creditease.dbus.stream.common.appender.bean.DbusDatasource;
import com.creditease.dbus.stream.common.appender.bean.EmitData;
import com.creditease.dbus.stream.common.appender.bean.MetaVersion;
import com.creditease.dbus.stream.common.appender.bolt.processor.BoltCommandHandler;
import com.creditease.dbus.stream.common.appender.bolt.processor.listener.CommandHandlerListener;
import com.creditease.dbus.stream.common.appender.cache.GlobalCache;
import com.creditease.dbus.stream.common.appender.enums.Command;
import com.creditease.dbus.stream.common.appender.utils.Pair;
import com.creditease.dbus.stream.common.appender.utils.PairWrapper;
import com.creditease.dbus.stream.common.appender.utils.Utils;
import com.google.common.base.Joiner;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.tuple.Tuple;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


/**
 * Created by ximeiwang on 2017/12/25.
 */
public class MongoWrapperDefaultHandler implements BoltCommandHandler {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private CommandHandlerListener listener;
    //private long lastPos = 0L;
    private Tuple tuple;
    private MetaVersion version;
    private long offset;
    private String groupId;
    private DataTable table;

    public MongoWrapperDefaultHandler(CommandHandlerListener listener) {
        this.listener = listener;
    }

    @Override
    public void handle(Tuple tuple) {
        EmitData emitData = (EmitData) tuple.getValueByField(Constants.EmitFields.DATA);
        //MetaVersion version = emitData.get(EmitData.VERSION);
        this.tuple = tuple;
        this.offset = emitData.get(EmitData.OFFSET);
        this.version = emitData.get(EmitData.VERSION);
        //this.groupId = groupField(version.getSchema(), version.getTable());
        //this.groupId = emitData.get(EmitData.GROUP_KEY);
        this.groupId = (String) tuple.getValueByField("group_field");
        this.table = emitData.get(EmitData.DATA_TABLE);

        String message = emitData.get(EmitData.MESSAGE);
        JSONObject entryJson = JSON.parseObject(message);

        int payloadCount = 0;
        int payloadSize = 0;
        int payloadMaxCount = getMaxCount();
        int payloadMaxSize = getMaxSize();

        //展开级别，现在是0级和1级。isOpen字段：0不展开,1一级展开
        int openDegree = table.getIsOpen() == 1 ? 1 : 0;
        //是否补全数据
        boolean isAutocomplete = table.getIsAutocomplete();
        DbusMessageBuilder builder = createBuilderWithSchema(message, openDegree);

        //EntryHeader header = null;
        long uniquePos = 0;

        //EncodeColumnProvider provider = new CachedEncodeColumnProvider(table.getId());
        //MessageEncoder encoder = new MessageEncoder();
        try {
            JSONObject document = entryJson.getJSONObject("_o");
            PairWrapper<String, Object> wrapper = new PairWrapper<>();
            wrapper.addProperties(Constants.MessageBodyKey.POS, entryJson.getString("_ts"));//TODO 这个取什么值呢？
            wrapper.addProperties(Constants.MessageBodyKey.OP_TS, entryJson.getString("_ts"));

            //paloads值获取
            String eventType = entryJson.getString("_op");
            if (document != null) {
                if (eventType.equals("u")) {
                    //补全数据操作
                    if (isAutocomplete) {
                        String schemaName = entryJson.getString("_ns").split("\\.")[0];
                        String tableName = entryJson.getString("_ns").split("\\.")[1];
                        Document sourceData = fetchData(schemaName, tableName, entryJson.getString("_id"));
                        sourceData.remove("_id");//TODO 后面会有统一的补齐

                        for (String key : sourceData.keySet()) {
                            String colType = StringUtils.substringAfterLast(
                                    sourceData.get(key).getClass().getTypeName(), ".");
                            if (colType.toLowerCase().equals("date")) {
                                wrapper.addPair(new Pair<>(key, formateDate2String(sourceData.getDate(key))));
                            }
                            wrapper.addPair(new Pair<>(key, sourceData.get(key)));
                        }
                    } else if (null != document.getString("$set")) {
                        JSONObject setFields = document.getJSONObject("$set");
                        for (String key : setFields.keySet()) {
                            wrapper.addPair(new Pair<>(key, setFields.get(key)));
                        }
                    }
                } else {
                    for (String key : document.keySet()) {
                        wrapper.addPair(new Pair<>(key, document.get(key)));
                    }
                }
                /*for (String key : document.keySet()) {
                    //wrapper.addPair(new Pair<>(key, CharSequence.class.isInstance(document.get(key))?document.getString(key):document.get(key)));
                    wrapper.addPair(new Pair<>(key, document.get(key)));
                }*/
            }

            long btime = entryJson.getLong("_ts");
            long timepart = btime >> 32;
            //long incpart = btime & 0xffffffff;
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String time_ts = format.format(new Date((long) timepart * 1000L));

            List<Object> payloads = new ArrayList<>();
            payloads.add(entryJson.get("_ts")); // ums_id
            //payloads.add(entryJson.get("_ts")); // ums_ts
            payloads.add(time_ts + ".000"); // ums_ts
            payloads.add(entryJson.getString("_op"));// ums_op
            payloads.add(generateUmsUid());// ums_uid

            if (openDegree == 0) {//如果是0级展开
                //"_id"和其他字段作为一个jsonObj
                wrapper.addPair(new Pair<>("_id", entryJson.getString("_id")));
                payloadSize += addPayloadColumnsWithDegree(payloads, entryJson.getString("_o"), wrapper, openDegree);
                builder.appendPayload(payloads.toArray());//TODO 数据格式的转换，这个怎么办，mongo，JSon，Java格式转换
            } else {
                payloads.add(entryJson.getString("_id"));// 先添加，保持和schema中field顺序一致
                payloadSize += addPayloadColumns(payloads, (String) entryJson.getString("_o"), wrapper);
                builder.appendPayload(payloads.toArray());//TODO 数据格式的转换，这个怎么办，mongo，JSon，Java格式转换
            }

            payloadCount++;

            // 判断message是否包含payload,如果payload列表为空则不写kafka
            if (payloadCount >= payloadMaxCount) {
                logger.debug("Payload count out of limitation[{}]!", payloadMaxCount);
                //encoder.encode(builder.getMessage(), provider);
                emitMessage(builder.getMessage());
                builder = createBuilderWithSchema(message, openDegree);
                payloadCount = 0;
            } else if (payloadSize >= payloadMaxSize) {
                logger.debug("Payload size out of limitation[{}]!", payloadMaxSize);
                //encoder.encode(builder.getMessage(), provider);
                emitMessage(builder.getMessage());
                builder = createBuilderWithSchema(message, openDegree);
                payloadSize = 0;
            }
        } catch (Exception e) {
            long errId = System.currentTimeMillis();
            logger.error("Build dbus message error, abort this message, {}", e.getMessage(), e);
            //BoltCommandHandlerHelper.onBuildMessageError(errId + "", version, e);
        }
        // 判断message是否包含payload,如果payload列表为空则不写kafka
        DbusMessage messages = builder.getMessage();

        //TODO Mongo 可以不用encode加盐吧 so
        //encoder.encode(messages, provider);

        if (!messages.getPayload().isEmpty()) {
            emitMessage(builder.getMessage());
        }
    }

    /**
     * 创建DBusMessageBuilder对象,同时生成ums schema
     */
    private DbusMessageBuilder createBuilderWithSchema(String message, int openDegree) {
        DbusMessageBuilder builder = new DbusMessageBuilder(com.creditease.dbus.commons.Constants.VERSION_14);
        JSONObject entryJson = JSON.parseObject(message);
        //String namespace = entryJson.getString("_ns");
        String schemaName = entryJson.getString("_ns").split("\\.")[0];
        String tableName = entryJson.getString("_ns").split("\\.")[1];
        //String namespace = builder.buildNameSpace(Utils.getDataSourceNamespace(), schemaName, tableName, 0, "0");//TODO partition table name暂且设置成tablename相同，待以后maybe写到Sharding的时候用得着
        String namespace = buildNameSpace(Utils.getDataSourceNamespace(), schemaName, tableName, "*", "0");

        //builder.build(DbusMessage.ProtocolType.DATA_INCREMENT_DATA, namespace, table.getBatchId());

        //oplog.rs中每条记录的标记id
        //builder.appendSchema("_id", DataType.convertMongoDataType("String"), true);

        //根据eventType获取column信息
        String eventType = entryJson.getString("_op");//TODO mongo是否也需要，根据operation的不同，传过来的信息结构不同，进行处理一下呢

        if (eventType.equals("u")) {
            //handle update operation
            JSONObject document = entryJson.getJSONObject("_o");
            if (document == null) {
                logger.warn("_o information is null");
                return builder;
            }
            //判断是否有unset字段
            if (null != document.getString("$unset")) {
                List<DbusMessage.Field> unsetFields = new ArrayList<>();
                JSONObject unsetFiled = document.getJSONObject("$unset");
                for (String unsetKey : unsetFiled.keySet()) {
                    String colType = StringUtils.substringAfterLast(unsetFiled.get(unsetKey).getClass().getTypeName(), ".");
                    unsetFields.add(new DbusMessage.Field(unsetKey, DataType.convertMongoDataType(colType), true));
                }
                builder.build(DbusMessage.ProtocolType.DATA_INCREMENT_DATA, namespace, table.getBatchId(), unsetFields);
            } else {
                builder.build(DbusMessage.ProtocolType.DATA_INCREMENT_DATA, namespace, table.getBatchId());
            }

            switch (openDegree) {
                case 0:
                    /** 0级只添加一个field
                     * {
                     "encoded": false,
                     "name": "jsonObj",
                     "nullable": false,
                     "type": "jsonobject"
                     }
                     */
                    builder.appendSchema("jsonObj", DataType.JSONOBJECT, true);
                    break;
                case 1:
                default://TODO 目前只有0级和1级，除0级外，默认就是1级
                    //oplog.rs中每条记录的标记id
                    builder.appendSchema("_id", DataType.convertMongoDataType("String"), true);

                    JSONObject setFields;
                    if (table.getIsAutocomplete()) {
                        //如果需要补全数据，那么schema应该是原Collection中该Document的全部数据
                        Document sourceData = fetchData(schemaName, tableName, entryJson.getString("_id"));
                        if (sourceData != null) {
                            sourceData.remove("_id");// 已作为公共的部添加
                            for (String key : sourceData.keySet()) {
                                String colType = StringUtils.substringAfterLast(sourceData.get(key).getClass().getTypeName(), ".");
                                builder.appendSchema(key, DataType.convertMongoDataType(colType), true);
                                //builder.appendSchema(key, DataType.STRING, true);//TODO mongo支持的数据类型有很多，但是这里不知道要怎么去识别其数据类型
                            }

                        }
                    } else if (null != document.getString("$set")) {
                        setFields = document.getJSONObject("$set");
                        doBuildSchema(builder, setFields);
                    }
                    /*if (null != document.getString("$set")) {
                        JSONObject setFileds = document.getJSONObject("$set");
                        for (String setKey : setFileds.keySet()) {
                            String colType = StringUtils.substringAfterLast(setFileds.get(setKey).getClass().getTypeName(), ".");
                            builder.appendSchema(setKey, DataType.convertMongoDataType(colType), true);
                        }
                    }*/
            }
            /*for(String key : document.keySet()){
                if(key.equals("$set")) {
                    JSONObject setFiled = document.getJSONObject("$set");
                    for (String setKey : setFiled.keySet()) {
                        String colType = StringUtils.substringAfterLast(document.get(key).getClass().getTypeName(), ".");
                        builder.appendSchema(key, DataType.convertMongoDataType(colType), true);
                    }
                }else if(key.equals("$unset")){
                    JSONObject unsetFiled = document.getJSONObject("$unset");
                    for(String unsetKey : unsetFiled.keySet()){
                        String colType = StringUtils.substringAfterLast(unsetFiled.get(unsetKey).getClass().getTypeName(), ".");
                        builder.appendSchemaUnsetFiled(unsetKey, DataType.convertMongoDataType(colType), true);//TODO 定义schema的unsetfiled字段
                    }
                }else {
                    logger.error("Can't support this update classification : {}.", key);
                }
                }
            }*/
        } else if (eventType.equals("d") || eventType.equals("i")) {
            //handle remove operation or insert operation

            builder.build(DbusMessage.ProtocolType.DATA_INCREMENT_DATA, namespace, table.getBatchId());

            switch (openDegree) {
                case 0:
                    builder.appendSchema("jsonObj", DataType.JSONOBJECT, true);
                    break;
                case 1:
                default:
                    //oplog.rs中每条记录的标记id
                    builder.appendSchema("_id", DataType.convertMongoDataType("String"), true);
                    JSONObject document = entryJson.getJSONObject("_o");
                    doBuildSchema(builder, document);

            }

        } else {
            logger.error("Can't support this operation {}.", eventType);
        }

        return builder;
    }

    private String buildNameSpace(String datasourceNs, String dbSchema, String table, String ver, String partitionTable) {
        return Joiner.on(".").join(datasourceNs, dbSchema, table, ver, "0", partitionTable);
    }

    private String formateDate2String(Date date) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return formatter.format(date);
    }

    private int getMaxCount() {
        return PropertiesHolder.getIntegerValue(Constants.Properties.CONFIGURE,
                Constants.ConfigureKey.UMS_PAYLOAD_MAX_COUNT);
    }

    private int getMaxSize() {
        return PropertiesHolder.getIntegerValue(Constants.Properties.CONFIGURE,
                Constants.ConfigureKey.UMS_PAYLOAD_MAX_SIZE);
    }

    private String generateUmsUid() throws Exception {
        //生成ums_uid
        return String.valueOf(listener.getZkService().nextValue(Utils.join(".", Utils.getDatasource().getDsName(), Constants.UmsMessage.NAMESPACE_INDISTINCTIVE_SCHEMA, Constants.UmsMessage.NAMESPACE_INDISTINCTIVE_TABLE, Constants.UmsMessage.NAMESPACE_INDISTINCTIVE_VERSION)));
    }

    private int addPayloadColumns(List<Object> payloads, String columns, PairWrapper<String, Object> wrapper) throws Exception {
        int payloadSize = 0;
        if (columns == null)
            return payloadSize;

        List<Pair<String, Object>> pairs = wrapper.getPairs();
        for (Pair<String, Object> pair : pairs) {
            String value = pair.getValue().toString();
            payloads.add(value);
            if (value != null) {
                payloadSize += value.toString().getBytes("utf-8").length;
            }
        }
        return payloadSize;
        //TODO 为嘛要用下面这种方式写呢？觉得不用columns也是可以的啊
        /*
        JSONObject columnJson = JSON.parseObject(columns);
        for(String key : columnJson.keySet()) {
            Pair<String, Object> pair = wrapper.getPair(key);
            //Object value = pair.getValue();
            String value = pair.getValue().toString();
            payloads.add(value);
            if (value != null) {
                payloadSize += value.toString().getBytes("utf-8").length;
            }
        }
        return payloadSize;*/
    }

    /**
     * 0级，JSONObject
     *
     * @throws Exception
     */
    private int addPayloadColumnsWithDegree(List<Object> payloads, String columns, PairWrapper<String, Object> wrapper, int openDegree) throws Exception {
        int payloadSize = 0;
        if (columns == null) {
            //0级时，columns为null,但是wrapper的pairs中有"_id"
            if (openDegree != 0 || wrapper.getPairs().size() < 1)
                return payloadSize;
        }

        JSONObject columnsJson = new JSONObject();
        List<Pair<String, Object>> pairs = wrapper.getPairs();
        for (Pair<String, Object> pair : pairs) {
            columnsJson.put(pair.getKey(), pair.getValue());
        }
        if (columnsJson.size() > 0) {
            payloadSize = columnsJson.toJSONString().getBytes("utf-8").length;
            payloads.add(columnsJson.toJSONString());
        }

        return payloadSize;
    }

    private void emitMessage(DbusMessage message) {
        EmitData data = new EmitData();
        data.add(EmitData.MESSAGE, message);
        data.add(EmitData.VERSION, version);
        data.add(EmitData.GROUP_KEY, groupId);
        data.add(EmitData.OFFSET, offset);

        this.emit(listener.getOutputCollector(), tuple, groupId, data, Command.UNKNOWN_CMD);
    }

    /**
     * 根据oid去数据库回查数据
     *
     * @param oid
     * @return
     */
    private Document fetchData(String schemaName, String tableName, String oid) {
        Document result = null;
        DbusDatasource datasource = GlobalCache.getDatasource();
        MongoClientURI uri = new MongoClientURI(datasource.getMasterUrl());
        MongoClient client = new MongoClient(uri);
        MongoDatabase database = client.getDatabase(schemaName);
        MongoCollection<Document> collection = database.getCollection(tableName);
        MongoCursor<Document> cursor = collection.find(new BasicDBObject().append("_id", new ObjectId(oid))).iterator();
        if (cursor.hasNext()) {
            result = cursor.next();
        } else {
            logger.error("get source data error. schemaName:{}, tableName:{}, oid:{}", schemaName, tableName, oid);
        }
        client.close();
        return result;

    }

    private DbusMessageBuilder doBuildSchema(DbusMessageBuilder builder, JSONObject document) {
        if (document != null) {
            for (String key : document.keySet()) {
                String colType = StringUtils.substringAfterLast(document.get(key).getClass().getTypeName(), ".");
                builder.appendSchema(key, DataType.convertMongoDataType(colType), true);
                //builder.appendSchema(key, DataType.STRING, true);//TODO mongo支持的数据类型有很多，但是这里不知道要怎么去识别其数据类型
            }
        }
        return builder;
    }


    public static void main(String[] args) {
        /*List<Object> columns = new ArrayList<>();
         *//*
        "4340000580192308",
                "2018-08-17 15:54:49.000",
                "i",
                "30001",
         *//*
        columns.add("4340000580192308");
        columns.add("2018-08-17 15:54:49.000");
        columns.add("i");
        columns.add(30001);

        JSONObject ace = JSON.parseObject( "{ \"_id\" : { \"$oid\" : \"5bbf0c9e7c0e042ab4fb2667\" }, \"id\" : 1, \"name\" : \"bbbbb1\", \"age\" : 94, \"marriage\" : true, \"insertTime\" : { \"$date\" : 1539247262194 }, \"currentMillion\" : { \"$numberLong\" : \"1539247262194\" } }");
        columns.add(ace);

        columns.forEach(item -> {
            System.out.println(item.toString());
        });*/

        Document doc = new Document();
        doc.append("a", 1);
        doc.append("b", 2);
        doc.append("c", 3);

        doc.remove("b");
        doc.remove("d");

        System.out.println(doc.toJson());
    }
}
