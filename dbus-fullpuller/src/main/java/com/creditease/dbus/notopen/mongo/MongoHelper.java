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

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.common.FullPullConstants;
import com.creditease.dbus.helper.DBHelper;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;


public class MongoHelper {

    private static Logger logger = LoggerFactory.getLogger(MongoHelper.class);

    /**
     * 判断是否为单独的服务.即没有使用副本集或者分片
     *
     * @param mongoManager the connection
     * @return true if connected to a standalone server
     */
    public static boolean isStandalone(MongoManager mongoManager) {
        return !isReplicaSet(mongoManager) && !isSharded(mongoManager);
    }

    /**
     * 判断是否使用了副本集
     *
     * @param mongoManager
     * @return
     */
    public static boolean isReplicaSet(MongoManager mongoManager) {
        return runIsMaster(mongoManager).get("setName") != null;
    }

    /**
     * 执行isMaster命令，获取master相关信息
     *
     * @param mongoManager
     * @return
     */
    public static Document runIsMaster(MongoManager mongoManager) {
        return mongoManager.getDatabase("admin").runCommand(new Document("ismaster", 1));
    }

    /**
     * 判断是否使用了分片
     * 要检测你客户端连接的mongodb实例是否是mongos，可以使用 isMaster 命令。
     * 当客户端连上 mongos，isMaster 返回一个带有msg 字段的文档，且字段值为isdbgrid。
     *
     * @param mongoManager
     * @return
     */
    public static boolean isSharded(MongoManager mongoManager) {
        Document document = runIsMaster(mongoManager);
        Object msg = document.get("msg");
        return msg != null && msg.equals("isdbgrid");
    }

    public static boolean getMongoOpenFirst(JSONObject reqJson) {
        JSONObject payloadJson = reqJson.getJSONObject(FullPullConstants.REQ_PAYLOAD);
        Integer dsId = Integer.parseInt(payloadJson.getString("DBUS_DATASOURCE_ID"));
        String schemaName = payloadJson.getString("SCHEMA_NAME");
        String tableName = payloadJson.getString("TABLE_NAME");
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        int is_open = 0;
        try {
            conn = DBHelper.getDBusMgrConnection();
            String sql = "select t.is_open from t_data_tables t where t.ds_id = ? and t.schema_name = ? and t.table_name = ? ";
            ps = conn.prepareStatement(sql);
            ps.setInt(1, dsId);
            ps.setString(2, schemaName);
            ps.setString(3, tableName);
            rs = ps.executeQuery();
            if (rs.next()) {
                is_open = rs.getInt("is_open");
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            DBHelper.close(conn, ps, rs);
        }
        logger.info("[pull bolt] mongo一级节点展开开关状态:{}", is_open);
        return is_open == 0 ? false : true;
    }

    /**
     * 获取副本集中主机的hostname和port
     *
     * @param mongoManager    Mongo对象
     * @param stateStrToMatch 副本集中MongoDB节点的状态字符创(primary或者secondary)
     * @return 主机的hostname和port
     */
    /*public static List<String> getReplicaSetUrls(MongoManager mongoManager, String stateStrToMatch) {
        Document document = runReplicaSetStatusCommand(mongoManager);
        List urls = new ArrayList<String>();
        //遍历副本集中的成员
        for (Map<String, Object> member : (List<Map<String, Object>>) document.get("members")) {
            String name = (String) member.get("name");
            if (!name.contains(":")) {
                name = name + ":27017";
            }
            name = "mongodb://" + name;
            if (stateStrToMatch.equalsIgnoreCase((String) member.get("stateStr"))) {
                urls.add(name);
            }
        }
        if (!urls.isEmpty()) {
            return urls;
        } else {
            throw new IllegalStateException("No member found in state " + stateStrToMatch);
        }
    }*/

    /*public static Document runReplicaSetStatusCommand(MongoManager mongoManager) {
        Document document = mongoManager.getDatabase("admin").runCommand(new Document("replSetGetStatus", 1));
        String json = document.toJson();
        if (json != null && json.indexOf("--replSet") != -1) {
            System.err.println("---- SecondaryReadTest: This is not a replica set - not testing secondary reads");
            return null;
        }
        return document;
    }*/
}
