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


package com.creditease.dbus.notopen.mongo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.creditease.dbus.common.FullPullConstants;
import com.creditease.dbus.common.bean.DBConfiguration;
import com.creditease.dbus.handler.SplitHandler;
import com.creditease.dbus.helper.FullPullHelper;
import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.bson.Document;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Future;


/**
 * This is Description
 *
 * @author xiancangao
 * @date 2019/01/10
 */
public class MongoSplitHandler extends SplitHandler {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void executeSplit() throws Exception {
        MongoManager mongoManager = null;
        try {
            String mongoUrl = dbConf.getString(DBConfiguration.DataSourceInfo.URL_PROPERTY_READ_ONLY);
            String username = dbConf.getString(DBConfiguration.DataSourceInfo.USERNAME_PROPERTY);
            String password = dbConf.getString(DBConfiguration.DataSourceInfo.PASSWORD_PROPERTY);
            mongoManager = new MongoManager(mongoUrl, username, password);

            JSONObject reqJson = JSON.parseObject(reqString);
            String dsKey = FullPullHelper.getDataSourceKey(reqJson);

            String tableNameWithSchema = dbConf.getInputTableName();
            int qualifierIndex = tableNameWithSchema.indexOf('.');
            String schema = tableNameWithSchema.substring(0, qualifierIndex);
            String tableName = tableNameWithSchema.substring(qualifierIndex + 1);
            // 获取总行数
            long totalRowsCount = mongoManager.count(schema, tableName);
            //包装每一片信息，写kafka，供数据拉取进程使用
            long splitIndex = 0;
            Long firstShardOffset = null;
            Long lastShardOffset = null;
            RecordMetadata producedRecord = null;

            JSONObject splitWrapper = new JSONObject();
            // 1.单机版
            if (MongoHelper.isStandalone(mongoManager)) {
                logger.info("[split bolt]: 单机版 mongo ,一片拉取");
                MongoInputSplit inputSplit = new MongoInputSplit();
                // 拉数据的目标mongo host即为当前传进来的mongo host
                inputSplit.setPullTargetMongoUrl(mongoUrl);
                inputSplit.setTargetTableName(tableName);

                splitWrapper.put(FullPullConstants.DATA_CHUNK_SPLIT, JSON.toJSONString(inputSplit, SerializerFeature.WriteClassName));
                splitWrapper.put(FullPullConstants.DATA_CHUNK_SPLIT_INDEX, ++splitIndex);
                splitWrapper.put(FullPullConstants.FULLPULL_REQ_PARAM, reqString);

                String fullPullMediantTopic = commonProps.getProperty(FullPullConstants.FULL_PULL_MEDIANT_TOPIC);
                ProducerRecord record = new ProducerRecord<>(fullPullMediantTopic, dataType, JSON.toJSONString(splitWrapper).getBytes());
                Future<RecordMetadata> future = byteProducer.send(record);
                producedRecord = future.get();
                splitWrapper.clear();
                firstShardOffset = producedRecord.offset();
                lastShardOffset = producedRecord.offset();
                logger.info("[split bolt] {},生成第{}片分片 , 累计数据量{} ,query{}", dsKey, splitIndex, totalRowsCount, inputSplit.getDataQueryObject());
            }
            // 2.副本集
            if (MongoHelper.isReplicaSet(mongoManager)) {
                logger.info("[split bolt]: 副本集 mongo 集群,一片拉取");
                MongoInputSplit inputSplit = new MongoInputSplit();
                inputSplit.setPullTargetMongoUrl(mongoUrl);
                inputSplit.setTargetTableName(tableName);

                splitWrapper.put(FullPullConstants.DATA_CHUNK_SPLIT, JSON.toJSONString(inputSplit, SerializerFeature.WriteClassName));
                splitWrapper.put(FullPullConstants.DATA_CHUNK_SPLIT_INDEX, ++splitIndex);
                splitWrapper.put(FullPullConstants.FULLPULL_REQ_PARAM, reqString);

                String fullPullMediantTopic = commonProps.getProperty(FullPullConstants.FULL_PULL_MEDIANT_TOPIC);
                ProducerRecord record = new ProducerRecord<>(fullPullMediantTopic, dataType, JSON.toJSONString(splitWrapper).getBytes());
                Future<RecordMetadata> future = byteProducer.send(record);
                producedRecord = future.get();
                splitWrapper.clear();
                firstShardOffset = producedRecord.offset();
                lastShardOffset = producedRecord.offset();

                logger.info("[split bolt] {},生成第{}片分片 , 累计数据量{} ,query{}", dsKey, splitIndex, totalRowsCount, inputSplit.getDataQueryObject());
            }
            // 3.分片集群
            if (MongoHelper.isSharded(mongoManager)) {
                // 先获取各分片的主库地址
                Map<String, String> shardsUrls = getShardedUrls(mongoManager, username, password);
                // 查询要拉取的表是否开启了分片
                Document first = mongoManager.find(MongoConstant.DB_CONFIG, MongoConstant.CONFIG_COLLECTIONS,
                        new Document(MongoConstant._ID, tableNameWithSchema)).first();
                //开启了分片,进行分片
                if (first != null) {
                    // 分两种情况处理：hash分片的情况，按shard并发拉取; range分片的情况，按shard及chunk拉取
                    List<MongoInputSplit> splitInfoByShard = getSplitInfoByShard(mongoManager, tableNameWithSchema, shardsUrls, (Document) first.get(MongoConstant.KEY));
                    for (MongoInputSplit inputSplit : splitInfoByShard) {
                        inputSplit.setTargetTableName(tableName);

                        splitWrapper.put(FullPullConstants.DATA_CHUNK_SPLIT, JSON.toJSONString(inputSplit, SerializerFeature.WriteClassName));
                        splitWrapper.put(FullPullConstants.DATA_CHUNK_SPLIT_INDEX, ++splitIndex);
                        splitWrapper.put(FullPullConstants.FULLPULL_REQ_PARAM, reqString);

                        String fullPullMediantTopic = commonProps.getProperty(FullPullConstants.FULL_PULL_MEDIANT_TOPIC);
                        ProducerRecord record = new ProducerRecord<>(fullPullMediantTopic, dataType, JSON.toJSONString(splitWrapper).getBytes());
                        Future<RecordMetadata> future = byteProducer.send(record);
                        producedRecord = future.get();
                        splitWrapper.clear();
                        if (splitIndex == 1) {
                            firstShardOffset = producedRecord.offset();
                        }
                        logger.info("[split bolt] {},生成第{}片分片 , 累计数据量{} ,query{}", dsKey, splitIndex, totalRowsCount, inputSplit.getDataQueryObject());
                    }
                    lastShardOffset = producedRecord.offset();
                } else {
                    //FindIterable<Document> documents = mongoManager.find(DB_CONFIG, CONFIG_KEY_DBS, new Document(KEY_ID, schema));
                    //String shardName = documents.first().getString(KEY_PRIMARY);
                    //String replicaHostOfShard = shardsUrls.get(shardName);
                    MongoInputSplit inputSplit = new MongoInputSplit();
                    // 拉数据的目标mongo host为:上面获取到的副本或主库的任一地址。所以get(0)
                    inputSplit.setPullTargetMongoUrl(mongoUrl);

                    splitWrapper.put(FullPullConstants.DATA_CHUNK_SPLIT, JSON.toJSONString(inputSplit, SerializerFeature.WriteClassName));
                    splitWrapper.put(FullPullConstants.DATA_CHUNK_SPLIT_INDEX, ++splitIndex);
                    splitWrapper.put(FullPullConstants.FULLPULL_REQ_PARAM, reqString);

                    String fullPullMediantTopic = commonProps.getProperty(FullPullConstants.FULL_PULL_MEDIANT_TOPIC);
                    ProducerRecord record = new ProducerRecord<>(fullPullMediantTopic, dataType, JSON.toJSONString(splitWrapper).getBytes());
                    Future<RecordMetadata> future = byteProducer.send(record);
                    producedRecord = future.get();
                    splitWrapper.clear();

                    firstShardOffset = producedRecord.offset();
                    lastShardOffset = producedRecord.offset();
                    logger.info("[split bolt] {},生成第{}片分片 , 累计数据量{} ,query{}", dsKey, splitIndex, totalRowsCount, inputSplit.getDataQueryObject());
                }
            }

            //向monitor节点写分片信息
            writeSplitResultToZkAndDB(0L, splitIndex, totalRowsCount, firstShardOffset, lastShardOffset);
            logger.info("[split bolt] {} 生成分片完毕，累计分片数量{},累计数据量{}", dsKey, splitIndex, totalRowsCount);
        } catch (Exception e) {
            logger.error("Exception happened when splitting data shards for mongo.", e);
            throw e;
        } finally {
            if (mongoManager != null) {
                mongoManager.close();
            }
        }
    }

    /**
     * 查询每个shard 的主库地址
     */
    private Map<String, String> getShardedUrls(MongoManager mongoManager, String username, String password) {
        FindIterable<Document> documents = mongoManager.find("config", "shards");
        Map<String, String> shardsUrlMap = new HashMap<>();
        documents.forEach(new Block<Document>() {
            @Override
            public void apply(Document document) {
                //格式：shard1/localhost:27017,localhost:27018,localhost:27019
                String[] shardHosts = document.getString("host").split("/");
                String shardName = shardHosts[0];
                String url = shardHosts[1];

                shardsUrlMap.put(shardName, url);
            }
        });
        return shardsUrlMap;
    }

    private List<MongoInputSplit> getSplitInfoByShard(MongoManager mongoManager, String tableNameWithSchema, Map<String, String> shardsUrls, Document shardKeys) {
        List<MongoInputSplit> inputSplitList = new ArrayList<>();
        //hash分片,按照shard分片拉取
        if (shardKeys.values().contains(MongoConstant.SHARD_TYPE_HASH)) {
            for (String shard : shardsUrls.keySet()) {
                String url = shardsUrls.get(shard);
                MongoInputSplit mongoInputSplit = new MongoInputSplit();
                mongoInputSplit.setPullTargetMongoUrl(url);
                inputSplitList.add(mongoInputSplit);
            }
            logger.info("doSplitting(): shard分片 mongo 集群,hash分片,按照shard分片拉取");
        } else {
            //range分片：按shard按chunk拉取。
            // 查询chunks信息，获取chunk上下界，然后根据chunks上下界分片、拉取。
            FindIterable<Document> chunksInfo = mongoManager.find(MongoConstant.DB_CONFIG, MongoConstant.CONFIG_CHUNKS,
                    new Document(MongoConstant.NS, tableNameWithSchema));
            // 遍历shards，获取上下界信息，生成带查询条件的split
            chunksInfo.forEach(new Block<Document>() {
                @Override
                public void apply(Document document) {
                    String shard = document.get(MongoConstant.SHARD).toString();
                    String url = shardsUrls.get(shard);
                    Set<Map.Entry<String, Object>> minEntry = ((Document) document.get("min")).entrySet();
                    Set<Map.Entry<String, Object>> maxEntry = ((Document) document.get("max")).entrySet();

                    ArrayList<Document> queryList = new ArrayList<>();
                    for (Map.Entry<String, Object> entry : minEntry) {
                        String key = entry.getKey();
                        Object value = entry.getValue();
                        if (!(value instanceof MinKey)) {
                            Document query = new Document();
                            query.put(key, new Document("$gte", value));
                            queryList.add(query);
                        }
                    }
                    for (Map.Entry<String, Object> entry : maxEntry) {
                        String key = entry.getKey();
                        Object value = entry.getValue();
                        if (!(value instanceof MaxKey)) {
                            Document query = new Document();
                            query.put(key, new Document("$lt", value));
                            queryList.add(query);
                        }
                    }
                    Document query = new Document();
                    query.put("$and", queryList);

                    MongoInputSplit mongoInputSplit = new MongoInputSplit();
                    mongoInputSplit.setPullTargetMongoUrl(url);
                    mongoInputSplit.setDataQueryObject(query);
                    inputSplitList.add(mongoInputSplit);
                }
            });
            logger.info("doSplitting(): shard分片 mongo 集群,range分片,按照shard和chunk分片拉取");
        }
        return inputSplitList;
    }
}
