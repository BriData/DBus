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

package com.creditease.dbus.heartbeat.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.creditease.dbus.heartbeat.container.DataSourceContainer;
import com.creditease.dbus.heartbeat.dao.IHeartBeatDao;
import com.creditease.dbus.heartbeat.log.LoggerFactory;
import com.creditease.dbus.heartbeat.util.DBUtil;
import com.creditease.dbus.heartbeat.util.DateUtil;

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

    //保留最后的 10000个心跳信息
    private String getDeleteOldHeartBeat() {
        return "delete from db_heartbeat_monitor where id < ?";
    }

    @Override
    public int sendPacket(String key, String dsName, String schemaName, String tableName, String packet, boolean isMysql) {
        Connection conn = null;
        PreparedStatement ps = null;
        int cnt = 0;
        try {
            conn = DataSourceContainer.getInstance().getConn(key);
            if (isMysql) {
                ps = conn.prepareStatement(getSendPacketSql2Mysql());
                ps.setString(1, dsName);
                ps.setString(2, schemaName);
                ps.setString(3, tableName);
                ps.setString(4, packet);
                ps.setString(5, DateUtil.convertLongToStr4Date(System.currentTimeMillis()));
                ps.setString(6, DateUtil.convertLongToStr4Date(System.currentTimeMillis()));
            } else {
                ps = conn.prepareStatement(getSendPacketSql2Oracle());
                ps.setString(1, dsName);
                ps.setString(2, schemaName);
                ps.setString(3, tableName);
                ps.setString(4, packet);
            }
            cnt = ps.executeUpdate();
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[db-HeartBeatDao]", e);
        } finally {
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        if (cnt == 1) {
            LoggerFactory.getLogger().info("[db-HeartBeatDao] 数据源: " + key + ", 插入心跳包成功. " + packet);
        } else {
            LoggerFactory.getLogger().error("[db-HeartBeatDao]: 数据源: " + key + ", 插入心跳包失败!" + packet);
        }
        return cnt;
    }
    @Override
    public int deleteOldHeartBeat(String key, boolean isMysql) {
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

}
