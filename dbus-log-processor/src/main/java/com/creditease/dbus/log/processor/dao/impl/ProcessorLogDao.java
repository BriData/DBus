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


package com.creditease.dbus.log.processor.dao.impl;

import com.creditease.dbus.log.processor.container.DataSourceContainer;
import com.creditease.dbus.log.processor.dao.IProcessorLogDao;
import com.creditease.dbus.log.processor.util.DBUtil;
import com.creditease.dbus.log.processor.vo.DBusDataSource;
import com.creditease.dbus.log.processor.vo.RuleInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2017/9/27.
 */
public class ProcessorLogDao implements IProcessorLogDao {

    private static Logger logger = LoggerFactory.getLogger(ProcessorLogDao.class);

    @Override
    public DBusDataSource loadDBusDataSourceConf(String key, String dsName) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        DBusDataSource dbusDs = null;

        try {
            StringBuilder sql = new StringBuilder();
            sql.append(" SELECT ");
            sql.append("     id,");
            sql.append("     ds_name,");
            sql.append("     ds_type,");
            sql.append("     topic,");
            sql.append("     ctrl_topic");
            sql.append(" FROM ");
            sql.append("     t_dbus_datasource");
            sql.append(" WHERE");
            sql.append("     ds_name = ?");

            conn = DataSourceContainer.getInstance().getConn(key);
            ps = conn.prepareStatement(sql.toString());
            ps.setString(1, dsName);
            rs = ps.executeQuery();
            if (rs.next()) {
                dbusDs = new DBusDataSource();
                dbusDs.setId(rs.getLong("id"));
                dbusDs.setDsName(rs.getString("ds_name"));
                dbusDs.setDsType(rs.getString("ds_type"));
                dbusDs.setTopic(rs.getString("topic"));
                dbusDs.setControlTopic(rs.getString("ctrl_topic"));
            }

        } catch (Exception e) {
            logger.error("load dbus ds conf error.", e);
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        return dbusDs;
    }

    @Override
    public List<RuleInfo> loadAbortTableRuleInfo(String key, String dsName) {
        return loadRuleInfo(key, dsName, "abort");
    }

    @Override
    public List<RuleInfo> loadActiveTableRuleInfo(String key, String dsName) {
        return loadRuleInfo(key, dsName, "ok");
    }

    private List<RuleInfo> loadRuleInfo(String key, String dsName, String status) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<RuleInfo> ruleInfos = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        try {
            sql.append(" SELECT");
            sql.append("     dds.ds_name,");
            sql.append("     ds.schema_name,");
            sql.append("     dt.id table_id,");
            sql.append("     dt.table_name,");
            sql.append("     dt.output_topic,");
            sql.append("     v.version,");
            sql.append("     ifnull(g.id, -1) group_id,");
            sql.append("     g.group_name,");
            sql.append("     g.status,");
            sql.append("     ifnull(r.order_id, -1) order_id,");
            sql.append("     r.rule_type_name,");
            sql.append("     r.rule_grammar");
            sql.append(" FROM");
            sql.append("     t_dbus_datasource dds");
            sql.append("     INNER JOIN t_data_schema ds on dds.id = ds.ds_id and dds.ds_name = ? ");
            sql.append("     INNER JOIN t_data_tables dt on ds.id = dt.schema_id and dt.status = ?");
            sql.append("     INNER JOIN t_meta_version v on dt.ver_id = v.id");
            sql.append("     LEFT JOIN  t_plain_log_rule_group_version g ON dt.id = g.table_id and g.status = 'active' and dt.ver_id = g.ver_id");
            sql.append("     LEFT JOIN  t_plain_log_rules_version r ON g.id = r.group_id");
            sql.append("     ORDER BY table_id, group_id, order_id");
//            logger.info("Query rule：{}", sql);
            conn = DataSourceContainer.getInstance().getConn(key);
            ps = conn.prepareStatement(sql.toString());
            ps.setString(1, dsName);
            ps.setString(2, status);
            rs = ps.executeQuery();
            while (rs.next()) {
                RuleInfo ruleInfo = new RuleInfo();
                ruleInfo.setDsName(rs.getString("ds_name"));
                ruleInfo.setSchemaName(rs.getString("schema_name"));
                ruleInfo.setTableId(rs.getLong("table_id"));
                ruleInfo.setTableName(rs.getString("table_name"));
                ruleInfo.setOutputTopic(rs.getString("output_topic"));
                ruleInfo.setVersion(rs.getLong("version"));
                ruleInfo.setGroupId(rs.getLong("group_id"));
                ruleInfo.setGroupName(rs.getString("group_name"));
                ruleInfo.setStatus(rs.getString("status"));
                ruleInfo.setOrderId(rs.getInt("order_id"));
                ruleInfo.setRuleTypeName(rs.getString("rule_type_name"));
                ruleInfo.setRuleGrammar(rs.getString("rule_grammar"));
                ruleInfos.add(ruleInfo);
            }
        } catch (Exception e) {
            logger.error("load rule info conf error.", e);
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        return ruleInfos;
    }

    @Override
    public void updateTableStatus(String key, String dsName, String schemaName, String tableName, String status) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            StringBuilder sql = new StringBuilder();
            sql.append(" SELECT ");
            sql.append("     id");
            sql.append(" FROM ");
            sql.append("     t_dbus_datasource");
            sql.append(" WHERE");
            sql.append("     ds_name = ?");

            conn = DataSourceContainer.getInstance().getConn(key);
            ps = conn.prepareStatement(sql.toString());
            ps.setString(1, dsName);
            rs = ps.executeQuery();
            int dsId = -1;
            if (rs.next()) {
                dsId = rs.getInt("id");
            }

            sql = new StringBuilder();
            sql.append(" SELECT ");
            sql.append("     id");
            sql.append(" FROM ");
            sql.append("     t_data_schema");
            sql.append(" WHERE");
            sql.append("     ds_id = ?");
            sql.append("  AND ");
            sql.append("     schema_name = ?");
//            logger.info("Query rule：{}", sql);
            ps = conn.prepareStatement(sql.toString());
            ps.setLong(1, dsId);
            ps.setString(2, schemaName);
            rs = ps.executeQuery();
            int schemaId = -1;
            if (rs.next()) {
                schemaId = rs.getInt("id");
            }

            sql = new StringBuilder();
            sql.append(" UPDATE ");
            sql.append("     t_data_tables");
            sql.append(" SET ");
            sql.append("     status = ?");
            sql.append(" WHERE");
            sql.append("     schema_id = ?");
            sql.append(" AND ");
            sql.append("     table_name = ?");
//            logger.info("Query rule：{}", sql);
            ps = conn.prepareStatement(sql.toString());
            ps.setString(1, status);
            ps.setInt(2, schemaId);
            ps.setString(3, tableName);
            ps.executeUpdate();
        } catch (Exception e) {
            logger.error("load rule info conf error.", e);
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
    }
}
