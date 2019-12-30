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


package com.creditease.dbus.extractor.dao.impl;

import com.creditease.dbus.extractor.common.utils.DBUtil;
import com.creditease.dbus.extractor.container.DataSourceContainer;
import com.creditease.dbus.extractor.container.ExtractorConfigContainer;
import com.creditease.dbus.extractor.dao.ILoadDbusConfigDao;
import com.creditease.dbus.extractor.vo.ExtractorVo;
import com.creditease.dbus.extractor.vo.OutputTopicVo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class LoadDbusConfigDaoImpl implements ILoadDbusConfigDao {

    private final static Logger logger = LoggerFactory.getLogger(LoadDbusConfigDaoImpl.class);

    private String getQueryConfigSql() {
        StringBuilder sql = new StringBuilder();
        sql.append(" select ");
        sql.append("     ds_name,");
        sql.append("     ds_type,");
        sql.append("     topic, ");
        sql.append("     ctrl_topic ");
        sql.append(" from ");
        sql.append("     t_dbus_datasource ");
        sql.append("     where ds_name=? ");
        sql.append("     and ds_type=? ");
        return sql.toString();
    }

    @Override
    public Set<OutputTopicVo> queryOutputTopic(String key) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        Set<OutputTopicVo> set = new HashSet<OutputTopicVo>();
        ExtractorVo extVo = ExtractorConfigContainer.getInstances().getExtractorConfig();
        try {
            conn = DataSourceContainer.getInstances().getConn(key);
            ps = conn.prepareStatement(getQueryConfigSql());
            ps.setString(1, extVo.getDbName());
            ps.setString(2, extVo.getDbType());
            rs = ps.executeQuery();
            while (rs.next()) {
                OutputTopicVo vo = new OutputTopicVo();
                vo.setDsName(rs.getString("ds_name"));
                vo.setDsType(rs.getString("ds_type"));
                vo.setTopic(rs.getString("topic"));
                vo.setControlTopic(rs.getString("ctrl_topic"));
                set.add(vo);
            }
        } catch (Exception e) {
            logger.error("[db-LoadDbusConfigDao]", e);
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        return set;
    }

    private String getQueryActiveTableSql() {
        StringBuilder sql = new StringBuilder();
        //select dbus.ds_name, tds.schema_name, tdt.table_name from t_dbus_datasource dbus,  t_data_schema tds,  t_data_tables tdt \
        //where dbus.id = tds.ds_id and dbus.status = 'active' and tds.status = 'active' and tds.id = tdt.schema_id and tdt.status <> 'inactive'
        sql.append(" select ");
        sql.append("     tds.schema_name,");
        sql.append("     tdt.table_name,");
        sql.append("     tdt.table_name_alias,");
        sql.append("     tdt.physical_table_regex");
        sql.append(" from ");
        sql.append("     t_dbus_datasource dbus, ");
        sql.append("     t_data_schema tds, ");
        sql.append("     t_data_tables tdt ");
        sql.append(" where ");
        sql.append("     dbus.id = tds.ds_id");
        sql.append("     and dbus.ds_name = ?");
        sql.append("     and dbus.status = 'active'");
        sql.append("     and tds.schema_name <> 'dbus'");
        sql.append("     and tds.status = 'active'");
        sql.append("     and tds.id = tdt.schema_id");
        sql.append("     and tdt.status <> 'inactive'");
        return sql.toString();
    }

    @Override
    public List<String> queryActiveTable(String dsName, String key) {
        List<String> ret = new ArrayList<>();
        StringBuilder activeTables = new StringBuilder();
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = DataSourceContainer.getInstances().getConn(key);
            ps = conn.prepareStatement(getQueryActiveTableSql());
            ps.setString(1, dsName);
            rs = ps.executeQuery();
            StringBuilder activeTable = new StringBuilder();
            StringBuilder tablePartitionRegex = new StringBuilder();
            while (rs.next()) {
                //activeTable.append(rs.getString("ds_name"));
                //activeTable.append(".");
                activeTable.append(rs.getString("schema_name"));
                activeTable.append("\\.");
                activeTable.append(rs.getString("physical_table_regex"));

                activeTables.append(activeTable.toString());
                activeTables.append(",");
                activeTable.delete(0, activeTable.length());

                tablePartitionRegex.append(rs.getString("table_name_alias"));
                tablePartitionRegex.append(".");
                tablePartitionRegex.append(rs.getString("physical_table_regex"));
                tablePartitionRegex.append(",");
            }
            activeTables.append("dbus\\..*");

            if (tablePartitionRegex.length() > 0) {
                tablePartitionRegex.delete(tablePartitionRegex.length() - 1, tablePartitionRegex.length());
            }

            ret.add(activeTables.toString());
            ret.add(tablePartitionRegex.toString());

        } catch (Exception e) {
            logger.error("[db-LoadDbusConfigDao]", e);
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        logger.info("[db-LoadDbusConfigDao] key: " + key + ", Active tables is " + activeTables.toString());
        return ret;
    }

}
