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

package com.creditease.dbus.heartbeat.dao.impl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.util.Map;

import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.heartbeat.container.DataSourceContainer;
import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.container.MongoClientContainer;
import com.creditease.dbus.heartbeat.dao.IHeartBeatDao;
import com.creditease.dbus.heartbeat.exception.SQLTimeOutException;
import com.creditease.dbus.heartbeat.log.LoggerFactory;
import com.creditease.dbus.heartbeat.mongo.DBusMongoClient;
import com.creditease.dbus.heartbeat.util.Constants;
import com.creditease.dbus.heartbeat.util.DBUtil;
import com.creditease.dbus.heartbeat.util.DateUtil;
import com.creditease.dbus.heartbeat.vo.HeartBeatMonitorVo;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mysql.jdbc.exceptions.MySQLTimeoutException;

import org.apache.commons.lang.StringUtils;
import org.bson.Document;

public class HeartBeatDaoImpl implements IHeartBeatDao {

    private String getSendPacketSql2Oracle() {
        StringBuilder sql = new StringBuilder();
        sql.append(" insert into ");
        sql.append("     db_heartbeat_monitor (ID, DS_NAME, SCHEMA_NAME, TABLE_NAME, PACKET, CREATE_TIME)");
        sql.append(" values (SEQ_HEARTBEAT_MONITOR.NEXTVAL, ?, ?, ?, ?, to_char(systimestamp, 'yyyymmdd hh24:mi:ss.ff6'))");
        return sql.toString();
    }

    private String getSendPacketSql2Mysql() {
        StringBuilder sql = new StringBuilder();
        sql.append(" insert into ");
        sql.append("     db_heartbeat_monitor (DS_NAME, SCHEMA_NAME, TABLE_NAME, PACKET, CREATE_TIME, UPDATE_TIME)");
        sql.append(" values (?, ?, ?, ?, ?, ?)");
        return sql.toString();
    }


    private String getMaxID() {
        return "select max(id) as maxID from db_heartbeat_monitor";
    }

    private String getMaxID2DB2() {
        return "select max(id) as maxID from dbus.db_heartbeat_monitor";
    }

    //保留最后的 10000个心跳信息
    private String getDeleteOldHeartBeat() {
        return "delete from db_heartbeat_monitor where id < ?";
    }


    @Override
    public int sendPacket(String key, String dsName, String schemaName, String tableName, String packet, String dsType) {
        int cnt = 0;
        if (DbusDatasourceType.stringEqual(dsType, DbusDatasourceType.MONGO)) {
            cnt = sendNoSqlPacket(key, dsName, schemaName, tableName, packet, dsType);
        } else if (DbusDatasourceType.stringEqual(dsType, DbusDatasourceType.MYSQL) ||
                   DbusDatasourceType.stringEqual(dsType, DbusDatasourceType.ORACLE)
                ) {
            cnt = sendRdbmsPacket(key, dsName, schemaName, tableName, packet, dsType);
        }
        return cnt;
    }

    private int sendNoSqlPacket(String key, String dsName, String schemaName, String tableName, String packet, String dsType) {
        int cnt = 0;
        DBusMongoClient dbusMongoClient = null;
        try {
            dbusMongoClient = MongoClientContainer.getInstance().getMongoClient(key);
            Document record = new Document();
            record.put("DS_NAME", dsName);
            record.put("SCHEMA_NAME", schemaName);
            record.put("TABLE_NAME", tableName);
            record.put("PACKET", packet);
            record.put("CREATE_TIME", DateUtil.convertLongToStr4Date(System.currentTimeMillis()));
            record.put("UPDATE_TIME", DateUtil.convertLongToStr4Date(System.currentTimeMillis()));
            if (dbusMongoClient.getShardMongoClients() != null &&
                dbusMongoClient.getShardMongoClients().size() > 0) {
                if (dbusMongoClient.isShardCollection(schemaName, tableName)) {
                    for (Map.Entry<String, MongoClient> client : dbusMongoClient.getShardMongoClients().entrySet()) {
                        MongoDatabase db = client.getValue().getDatabase("dbus");
                        MongoCollection collection = db.getCollection("db_heartbeat_monitor");
                        record.put("_id", dbusMongoClient.nextSequence("db_heartbeat_monitor"));
                        collection.insertOne(record);
                        cnt++;
                    }
                } else {
                    String majorShard = dbusMongoClient.getDbMajorShard(schemaName);
                    if (StringUtils.isNotBlank(majorShard)) {
                        MongoClient client = dbusMongoClient.getShardMongoClients().get(majorShard);
                        MongoDatabase db = client.getDatabase("dbus");
                        MongoCollection collection = db.getCollection("db_heartbeat_monitor");
                        record.put("_id", dbusMongoClient.nextSequence("db_heartbeat_monitor"));
                        collection.insertOne(record);
                        cnt++;
                    }
                }
            } else {
                MongoClient client = dbusMongoClient.getMongoClient();
                MongoDatabase db = client.getDatabase("dbus");
                MongoCollection collection = db.getCollection("db_heartbeat_monitor");
                record.put("_id", dbusMongoClient.nextSequence("db_heartbeat_monitor"));
                collection.insertOne(record);
                cnt++;
            }
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[db-HeartBeatDao]", e);
        }
        if (cnt >= 1) {
             LoggerFactory.getLogger().info("[db-HeartBeatDao] 数据源: " + key + ", 插入心跳包成功. " + packet);
        } else {
             LoggerFactory.getLogger().error("[db-HeartBeatDao]: 数据源: " + key + ", 插入心跳包失败!" + packet);
        }
        return cnt;
    }

    private int sendRdbmsPacket(String key, String dsName, String schemaName, String tableName, String packet, String dsType) {
        Connection conn = null;
        PreparedStatement ps = null;
        int cnt = 0;
        long beginConn = 0, endConn = 0, beginStmt = 0, endStmt = 0, closeConn = 0, closeEndConn = 0;
        try {
            beginConn = System.currentTimeMillis();
            conn = DataSourceContainer.getInstance().getConn(key);
            endConn = System.currentTimeMillis();
            if (StringUtils.equals(Constants.CONFIG_DB_TYPE_MYSQL, dsType)) {
                ps = conn.prepareStatement(getSendPacketSql2Mysql());
                ps.setString(1, dsName);
                ps.setString(2, schemaName);
                ps.setString(3, tableName);
                ps.setString(4, packet);
                ps.setString(5, DateUtil.convertLongToStr4Date(System.currentTimeMillis()));
                ps.setString(6, DateUtil.convertLongToStr4Date(System.currentTimeMillis()));
            } else if (StringUtils.equals(Constants.CONFIG_DB_TYPE_ORA, dsType)) {
                ps = conn.prepareStatement(getSendPacketSql2Oracle());
                ps.setString(1, dsName);
                ps.setString(2, schemaName);
                ps.setString(3, tableName);
                ps.setString(4, packet);
            }

            Integer queryTimeout = HeartBeatConfigContainer.getInstance().getHbConf().getQueryTimeout();
            if (queryTimeout == null) queryTimeout = 2;
            ps.setQueryTimeout(queryTimeout);

            beginStmt = System.currentTimeMillis();
            cnt = ps.executeUpdate();
            endStmt = System.currentTimeMillis();
        } catch (Exception e) {
            if (e instanceof MySQLTimeoutException) {
                throw new SQLTimeOutException(e.getMessage(), e);
            } else if (e instanceof SQLException) {
                SQLException sqle = (SQLException) e;
                if (sqle.getNextException() instanceof SQLRecoverableException) {
                    SQLRecoverableException sqlre = (SQLRecoverableException) sqle.getNextException();
                    if (sqle.getErrorCode() == 17060 && sqlre.getErrorCode() == 17002) {
                        throw new SQLTimeOutException(sqlre.getMessage(), sqlre);
                    }
                }
            }
            LoggerFactory.getLogger().error("[db-HeartBeatDao]", e);
        } finally {
            closeConn = System.currentTimeMillis();
            DBUtil.close(ps);
            DBUtil.close(conn);
            closeEndConn = System.currentTimeMillis();
        }

        String statTime = String.format(", beginConn: %d, ConUsed: %d, beginStmt: %d, stmtUsed: %d, closeConn: %d, closeUsed: %d",
                beginConn, endConn - beginConn, beginStmt, endStmt - beginStmt, closeConn, closeEndConn - closeConn);
        if (cnt == 1) {
            LoggerFactory.getLogger().info("[db-HeartBeatDao] 数据源: " + key + ", 插入心跳包成功. " + packet + statTime);
        } else {
            LoggerFactory.getLogger().error("[db-HeartBeatDao]: 数据源: " + key + ", 插入心跳包失败!" + packet + statTime);
        }
        return cnt;
    }

    @Override
    public int deleteOldHeartBeat(String key, String dsType) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        PreparedStatement ps2 = null;
        int cnt = 0;
        long maxID = 0;
        try {
            conn = DataSourceContainer.getInstance().getConn(key);

            //select maxID
                ps = conn.prepareStatement(getMaxID());
            rs = ps.executeQuery();
            while (rs.next()) {
                //read max id
                maxID = rs.getLong("maxID");
            }
            DBUtil.close(rs);
            DBUtil.close(ps);

            //delete old heartbeat record
                ps2 = conn.prepareStatement(getDeleteOldHeartBeat());
            ps2.setLong(1, maxID - 10000);
            cnt = ps2.executeUpdate();
            LoggerFactory.getLogger().info("[db-HeartBeatDao] 数据源: " + key + ", 删除旧心跳包成功. 条数=" + cnt);
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[db-HeartBeatDao] 删除旧心跳包失败." + key + ", ", e);
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(ps2);
            DBUtil.close(conn);
        }

        return cnt;
    }

    public static void main(String[] args) {
        //脱敏
        String zkServers = "dbus-z1:2181";
        Connection connection = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            connection = DriverManager.getConnection("jdbc:oracle:thin:@ (DESCRIPTION= (FAILOVER = yes)(ADDRESS = (PROTOCOL = TCP)(HOST =dbus-n1)(PORT = 1521))(ADDRESS = (PROTOCOL = TCP)(HOST =dbus-n2)(PORT = 1521)) (CONNECT_DATA =(SERVER = DEDICATED)(SERVICE_NAME = cedb)))", "cm", "cm9r");
            String sqlQuery = "select f_Str2List('aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddaaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,d,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddaaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,d,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddaaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,d,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddaaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,d,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddaaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,d,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddaaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,d,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddaaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,d,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd,fff,ddddaaa,bbb,ccc,ddd,eee,ddd,fff,ddddd,aaa,bbb,ccc,ddd,eee,ddd') as a from dual";

            stmt = connection.prepareStatement(sqlQuery);
            stmt.setQueryTimeout(2);



            System.out.println("begin ...");
//            int i = stmt.executeUpdate();
            rs = stmt.executeQuery();
            /*while (rs.next()) {
                System.out.println(rs.getString("DOC_ID"));
            }*/
            System.out.println("end!");

        } catch (Exception e) {
            e.printStackTrace();
            try {
                if (e instanceof SQLException && ((SQLException)(e)).getNextException() instanceof SQLRecoverableException) {
                    SQLException sqle = (SQLException) e;
                    if (sqle.getNextException() instanceof SQLRecoverableException) {
                        SQLRecoverableException sqlre = (SQLRecoverableException) sqle.getNextException();
                        if (sqle.getErrorCode() == 17060 && sqlre.getErrorCode() == 17002) {
                            throw new RuntimeException(sqlre.getMessage(), sqlre);
                        }
                    }
                }
            }catch (Exception innere) {
                innere.getCause();
            }

        } finally {

            if (rs != null) {
                try {
                    rs.close();
                } catch (Exception e) {
                }
            }

            if (stmt  != null) {
                try {
                    stmt.close();
                } catch (Exception e) {
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                }
            }
        }

    }

    private String getQueryHeartbeatSql2Mysql() {
        StringBuilder sql = new StringBuilder();
        sql.append(" select ");
        sql.append("     DS_NAME,");
        sql.append("     SCHEMA_NAME,");
        sql.append("     CREATE_TIME");
        sql.append(" from");
        sql.append("     db_heartbeat_monitor");
        sql.append(" where");
        sql.append("     DS_NAME = ? and");
        sql.append("     SCHEMA_NAME = ?");
        sql.append(" order by");
        sql.append("     CREATE_TIME desc");
        sql.append(" limit 1");
        return sql.toString();
    }


    private String getQueryHeartbeatSql2Oracle() {
        StringBuilder sql = new StringBuilder();
        sql.append(" select");
        sql.append("     wk.DS_NAME, wk.SCHEMA_NAME, wk.CREATE_TIME");
        sql.append(" from ");
        sql.append("     (select");
        sql.append("          DS_NAME,");
        sql.append("          SCHEMA_NAME,");
        sql.append("          CREATE_TIME");
        sql.append("      from");
        sql.append("          db_heartbeat_monitor");
        sql.append("      where");
        sql.append("          DS_NAME = ? and");
        sql.append("          SCHEMA_NAME = ?");
        sql.append("      order by");
        sql.append("          CREATE_TIME desc");
        sql.append("      ) wk");
        sql.append(" where");
        sql.append("     rownum = 1");
        return sql.toString();
    }

    @Override
    public HeartBeatMonitorVo queryLatestHeartbeat(String key, String dsName, String schemaName, String dsType) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        HeartBeatMonitorVo hbmVo = null;
        try {
            conn = DataSourceContainer.getInstance().getConn(key);
            if (StringUtils.equals(Constants.CONFIG_DB_TYPE_MYSQL, dsType)) {
                ps = conn.prepareStatement(getQueryHeartbeatSql2Mysql());
            } else if (StringUtils.equals(Constants.CONFIG_DB_TYPE_ORA, dsType)) {
                ps = conn.prepareStatement(getQueryHeartbeatSql2Oracle());
            }
            ps.setString(1, dsName);
            ps.setString(2, schemaName);

            rs = ps.executeQuery();
            if (rs.next()) {
                hbmVo = new HeartBeatMonitorVo();
                hbmVo.setDsName(rs.getString("DS_NAME"));
                hbmVo.setSchemaName(rs.getString("SCHEMA_NAME"));
                hbmVo.setCreateTime(rs.getString("CREATE_TIME"));
            }
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[db-HeartBeatDao]", e);
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        return hbmVo;
    }

}
