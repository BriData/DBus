package com.creditease.dbus.heartbeat.mongo;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.creditease.dbus.heartbeat.container.MongoClientContainer;
import com.creditease.dbus.heartbeat.dao.IHeartBeatDao;
import com.creditease.dbus.heartbeat.dao.impl.HeartBeatDaoImpl;
import com.creditease.dbus.heartbeat.util.JsonUtil;
import com.creditease.dbus.heartbeat.vo.JdbcVo;
import com.creditease.dbus.heartbeat.vo.PacketVo;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;

import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Administrator on 2018/11/8.
 */
public class DBusMongoClient {

    private static Logger logger = LoggerFactory.getLogger(DBusMongoClient.class);

    private boolean single = false;

    private boolean replSet = false;

    private boolean shard = false;

    private MongoClient mongoClient = null;

    private Map<String, MongoClient> shardMongoClients = null;

    private Map<String, String> dbMajorShardCache = new HashMap<>();

    public DBusMongoClient(JdbcVo jdbcVo) {
        mongoClient = new MongoClient(seeds(jdbcVo.getUrl()));
        shard = isShard();
        replSet = isReplSet();
        single = isSingle();
        obtainMajorShard();
        obtainShardMongoClients();
    }

    private List<ServerAddress> seeds(String url) {
        if (StringUtils.isBlank(url))
            throw new IllegalArgumentException("mongo url is empty.");
        List<ServerAddress> seeds = new ArrayList<>();
        for (String str : url.split(",")) {
            if (StringUtils.startsWith(str, "mongodb://")) {
                str = StringUtils.replace(str, "mongodb://", "");
            }
            String[] ipAndPort = StringUtils.split(str, ":");
            ServerAddress sa = new ServerAddress(new InetSocketAddress(ipAndPort[0], Integer.valueOf(ipAndPort[1])));
            seeds.add(sa);
        }

        return seeds;
    }

    private void obtainShardMongoClients() {
        if (shard) {
            shardMongoClients = new HashMap<>();
            MongoDatabase mdb = mongoClient.getDatabase("config");
            MongoCollection mongoCollection = mdb.getCollection("shards");
            FindIterable<Document> it = mongoCollection.find();
            for (Document doc : it) {
                // eg. { "_id" : "shard1", "host" : "shard1/192.168.0.1:27001,192.168.0.2:27001", "state" : 1 }
                logger.info("config.shards.find: {}", doc.toJson());
                String shard = StringUtils.substringBefore(doc.getString("host"), "/");
                String host = StringUtils.substringAfterLast(doc.getString("host"), "/");
                logger.info("{} host {}", doc.getString("_id"), host);
                shardMongoClients.put(shard, new MongoClient(seeds(host)));
            }
        }
    }

    private void obtainMajorShard() {
        MongoDatabase mdb = mongoClient.getDatabase("config");
        MongoCollection mongoCollection = mdb.getCollection("databases");
        FindIterable<Document> it = mongoCollection.find();
        for (Document doc : it) {
            // eg. { "_id" : "testdb", "primary" : "shard2", "partitioned" : true }
            logger.info("config.databases.find: {}", doc.toJson());
            dbMajorShardCache.put(doc.getString("_id"), doc.getString("primary"));
        }
    }

    private boolean isSingle() {
        boolean ret = false;
        if (!replSet && !shard)
            ret = true;
        return ret;
    }

    private boolean isReplSet() {
        boolean ret = false;
        DB db = new DB(mongoClient, "admin");
        CommandResult cr = db.command("replSetGetStatus");
        logger.info("isReplSet: {}", cr.toJson());
        if (cr.containsField("set") && cr.containsField("members")) {
            ret = true;
        }
        return ret;
    }

    private boolean isShard() {
        boolean ret = false;
        DB db = new DB(mongoClient, "admin");
        CommandResult cr = db.command("isdbgrid");
        logger.info("isShard: {}", cr.toJson());
        if (cr.containsField("isdbgrid") && cr.getInt("isdbgrid") == 1) {
            ret = true;
        }
        return ret;
    }

    public boolean isShardCollection(String db, String collection) {
        boolean ret = false;
        if (shard) {
            MongoDatabase mdb = mongoClient.getDatabase("config");
            MongoCollection mongoCollection = mdb.getCollection("collections");
            String id = StringUtils.join(new String[] {db, collection}, ".");
            Document condition = new Document("_id", id).append("dropped", false);
            ret = (mongoCollection.countDocuments(condition) > 0);
            logger.info("db:{}, collection:{} is shard collection.", db, collection);
        }
        return ret;
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public Map<String, MongoClient> getShardMongoClients() {
        return shardMongoClients;
    }

    public long nextSequence(String name) {
        MongoDatabase mdb = mongoClient.getDatabase("dbus");
        MongoCollection mongoCollection = mdb.getCollection("dbus_sequence");
        Document filter = new Document("_id", name);
        Document update = new Document("$inc", new Document("value", 1L));
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
        options.upsert(true);
        options.returnDocument(ReturnDocument.AFTER);
        Document doc = (Document) mongoCollection.findOneAndUpdate(filter, update, options);
        return doc.getLong("value");
    }

    public String getDbMajorShard(String db) {
        return dbMajorShardCache.get(db);
    }

    public static void main(String[] args) {
        JdbcVo jdbcVo = new JdbcVo();
        jdbcVo.setKey("test-mongo");
        jdbcVo.setUrl("vdbus-12:20000");
        MongoClientContainer.getInstance().register(jdbcVo);

        IHeartBeatDao dao = new HeartBeatDaoImpl();
        // {"node":"/DBus/HeartBeat/Monitor/bafang_sec/bas/sub_trade/0","time":1541662679484,"type":"checkpoint","txTime":1541662678866}
        PacketVo packet = new PacketVo();
        packet.setNode("/DBus/HeartBeat/Monitor/test-mongo/testdb/table1/0");
        packet.setType("checkpoint");
        packet.setTime(System.currentTimeMillis());
        packet.setTxTime(System.currentTimeMillis());
        String strPacket = JsonUtil.toJson(packet);
        dao.sendPacket(jdbcVo.getKey(), "test-mongo", "testdb", "table3", strPacket, "mongo");
        MongoClientContainer.getInstance().clear();
    }

}
