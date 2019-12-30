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

import com.creditease.dbus.common.FullPullConstants;
import com.creditease.dbus.helper.FullPullHelper;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Properties;

/**
 * This is Description
 *
 * @author xiancangao
 * @date 2018/09/18
 */
public class MongoManager {
    private Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * MongoClient的实例代表数据库连接池，是线程安全的，可以被多线程共享，客户端在多线程条件下仅维持一个实例即可
     * Mongo是非线程安全的，目前mongodb API中已经建议用MongoClient替代Mongo
     */
    private MongoClient mongoClient = null;


    public MongoManager(String url, String username, String password) {
        if (mongoClient == null) {
            Properties props = FullPullHelper.getFullPullProperties(FullPullConstants.ZK_NODE_NAME_MONGO_CONF, true);
            MongoClientOptions.Builder build = new MongoClientOptions.Builder();

            //与目标数据库能够建立的最大connection数量为5
            build.connectionsPerHost(Integer.valueOf(props.getProperty("connectionsPerHost")));
            //当链接空闲时,空闲线程池中最大链接数
            build.minConnectionsPerHost(Integer.valueOf(props.getProperty("minConnectionsPerHost")));
            //如果当前所有的connection都在使用中，则每个connection上可以有5个线程排队等待
            build.threadsAllowedToBlockForConnectionMultiplier(Integer.valueOf(props.getProperty("threadsAllowedToBlockForConnectionMultiplier")));
            // 一个线程等待链接可用的最大等待毫秒数，0表示不等待，负数表示等待时间不确定
            build.maxWaitTime(Integer.valueOf(props.getProperty("maxWaitTime")));
            //链接超时的毫秒数,0表示不超时,此参数只用在新建一个新链接时
            build.connectTimeout(Integer.valueOf(props.getProperty("connectTimeout")));
            //此参数表示socket I/O读写超时时间,推荐为不超时，即 0
            build.socketTimeout(Integer.valueOf(props.getProperty("socketTimeout")));

            MongoCredential credential = MongoCredential.createCredential(username, "admin", password.toCharArray());
            ArrayList<ServerAddress> serverAddresses = new ArrayList<>();


            for (String urlOne : url.split(",")) {
                String[] host_port = urlOne.split(":");
                serverAddresses.add(new ServerAddress(host_port[0], Integer.parseInt(host_port[1])));
            }
            try {
                mongoClient = new MongoClient(serverAddresses, credential, build.build());
            } catch (Exception e) {
                logger.error("Exception when new MongoClient.", e);
                throw e;
            }
        }
    }

    public MongoDatabase getDatabase(String dbName) {
        return mongoClient.getDatabase(dbName);
    }

    public MongoCollection<Document> getCollection(String dbName, String collectionName) {
        return getDatabase(dbName).getCollection(collectionName);
    }

    public FindIterable<Document> find(String dbName, String collectionName) {
        return getCollection(dbName, collectionName).find();
    }

    public FindIterable<Document> find(String dbName, String collectionName, Document document) {
        if (document == null) {
            return find(dbName, collectionName);
        }
        return getCollection(dbName, collectionName).find(document);
    }

    public Long count(String dbName, String collectionName) {
        return getCollection(dbName, collectionName).count();
    }

    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
        }
    }

}
