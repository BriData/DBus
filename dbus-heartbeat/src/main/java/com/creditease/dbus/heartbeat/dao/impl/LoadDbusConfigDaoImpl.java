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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.creditease.dbus.heartbeat.container.DataSourceContainer;
import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.dao.ILoadDbusConfigDao;
import com.creditease.dbus.heartbeat.log.LoggerFactory;
import com.creditease.dbus.heartbeat.util.Constants;
import com.creditease.dbus.heartbeat.util.DBUtil;
import com.creditease.dbus.heartbeat.vo.MonitorNodeVo;
import com.creditease.dbus.heartbeat.vo.DsVo;
import com.creditease.dbus.heartbeat.vo.TargetTopicVo;

public class LoadDbusConfigDaoImpl implements ILoadDbusConfigDao {

    private String getQuerySidConfigSql() {
        StringBuilder sql = new StringBuilder();
        sql.append(" select ");
        sql.append("     ds_name,");
        sql.append("     ds_type,");
        sql.append("     ds_desc,");
        sql.append("     master_url,");
        sql.append("     slave_url,");
        sql.append("     dbus_user,");
        sql.append("     dbus_pwd,");
        sql.append("     ctrl_topic");
        sql.append(" from ");
        sql.append("     t_dbus_datasource ");
        sql.append(" where ");
        sql.append("     status = 'active' ");
        return sql.toString();
    }

    @Override
    public List<DsVo> queryDsConfig(String key) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<DsVo> list = new ArrayList<DsVo>();
        try {
            conn = DataSourceContainer.getInstance().getConn(key);
            ps = conn.prepareStatement(getQuerySidConfigSql());
            rs = ps.executeQuery();
            while (rs.next()) {
                DsVo vo = new DsVo();
                vo.setUrl(rs.getString("master_url"));
                vo.setUserName(rs.getString("dbus_user"));
                vo.setPassword(rs.getString("dbus_pwd"));
                vo.setType(rs.getString("ds_type"));
                vo.setKey(rs.getString("ds_name"));
                vo.setCtrlTopic("ctrl_topic");
                if (StringUtils.equals(Constants.CONFIG_DB_TYPE_ORA, vo.getType())) {
                    vo.setDriverClass(Constants.DB_DRIVER_CLASS_ORA);
                } else if (StringUtils.equals(Constants.CONFIG_DB_TYPE_MYSQL, vo.getType())) {
                    vo.setDriverClass(Constants.DB_DRIVER_CLASS_MYSQL);
                }
                list.add(vo);
            }
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[db-LoadDbusConfigDao]", e);
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        LoggerFactory.getLogger().info("[db-LoadDbusConfigDao] key: " + key + ", 数据源数量： " + list.size());
        return list;
    }

    private String getQueryMonitorNodeSql() {
        StringBuilder sql = new StringBuilder();
        sql.append(" select ");
        sql.append("     dbus.ds_name,");
        sql.append("     tds.schema_name,");
        sql.append("     tdt.table_name");
        sql.append(" from ");
        sql.append("     t_dbus_datasource dbus, ");
        sql.append("     t_data_schema tds, ");
        sql.append("     t_data_tables tdt ");
        sql.append(" where ");
        sql.append("     dbus.id = tds.ds_id");
        sql.append("     and dbus.status = 'active'");
        sql.append("     and tds.schema_name not in (?)");
        sql.append("     and tds.id = tdt.schema_id");
        sql.append("     and tdt.status <> 'inactive'");
        return sql.toString();
    }

    @Override
    public Set<MonitorNodeVo> queryMonitorNode(String key) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        Set<MonitorNodeVo> list = new HashSet<MonitorNodeVo>();
        try {
            conn = DataSourceContainer.getInstance().getConn(key);
            ps = conn.prepareStatement(getQueryMonitorNodeSql());
            ps.setString(1, HeartBeatConfigContainer.getInstance().getHbConf().getExcludeSchema());
            Set<String> excludeSchema = getExcludeDbSchema(HeartBeatConfigContainer.getInstance().getHbConf().getExcludeSchema());
            rs = ps.executeQuery();
            while (rs.next()) {
                MonitorNodeVo vo = new MonitorNodeVo();
                vo.setDsName(rs.getString("ds_name"));
                vo.setSchema(rs.getString("schema_name"));
                vo.setTableName(rs.getString("table_name"));
                if(!isContainedByExcludeSchema(excludeSchema,vo.getDsName(),vo.getSchema()))
                	list.add(vo);
            }
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[db-LoadDbusConfigDao]", e);
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        LoggerFactory.getLogger().info("[db-LoadDbusConfigDao] key: " + key + ", schema数量 " + list.size());
        return list;
    }

    private String queryTargetTopicSql() {
        StringBuilder sql = new StringBuilder();
        sql.append(" select ");
        sql.append("     dbus.ds_name,");
        sql.append("     tdt.schema_name,");
        sql.append("     tdt.table_name,");
        sql.append("     tdt.output_topic");
        sql.append(" from ");
        sql.append("     t_dbus_datasource dbus, ");
        sql.append("     t_data_schema tds, ");
        sql.append("     t_data_tables tdt ");
        sql.append(" where ");
        sql.append("     dbus.id = tds.ds_id");
        sql.append("     and dbus.status = 'active'");
        sql.append("     and tds.schema_name not in (?)");
        sql.append("     and tds.id = tdt.schema_id");
        sql.append("     and tdt.status <> 'inactive'");
        sql.append("     and tdt.output_topic != ''");

        return sql.toString();
    }

    @Override
    public Set<TargetTopicVo> queryTargetTopic(String key) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        Set<TargetTopicVo> list = new HashSet<TargetTopicVo>();
        try {
            conn = DataSourceContainer.getInstance().getConn(key);
            ps = conn.prepareStatement(queryTargetTopicSql());
            ps.setString(1, HeartBeatConfigContainer.getInstance().getHbConf().getExcludeSchema());
            rs = ps.executeQuery();
            Set<String> excludeSchema = getExcludeDbSchema(HeartBeatConfigContainer.getInstance().getHbConf().getExcludeSchema());
            while (rs.next()) {
            	TargetTopicVo vo = new TargetTopicVo();
            	vo.setDsName(rs.getString("ds_name"));
            	vo.setSchemaName(rs.getString("schema_name"));
            	vo.setTableName(rs.getString("table_name"));
            	vo.setTargetTopic(rs.getString("output_topic"));
            	if(!isContainedByExcludeSchema(excludeSchema,vo.getDsName(),vo.getSchemaName()))
            		list.add(vo);
            }
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[db-LoadDbusConfigDao]", e);
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        LoggerFactory.getLogger().info("[db-LoadDbusConfigDao] key: " + key + ", target topic数量 " + list.size());
        return list;
    }

    private Set<String> getExcludeDbSchema(String excludeSchema){
    	String[] schema = StringUtils.split(excludeSchema, ",");
    	Set<String> schemaSet = new HashSet<String>();
    	schemaSet.addAll(Arrays.asList(schema));
    	return schemaSet;
    }

    private boolean isContainedByExcludeSchema(Set<String> excludeSchema, String dbName, String schemaName){
    	if(excludeSchema==null)
    		return false;
    	String dbSchema = StringUtils.join(new String[]{dbName,schemaName},".");
    	if(excludeSchema.contains(dbSchema)||excludeSchema.contains(schemaName))
    		return true;
    	return false;
    }

}
