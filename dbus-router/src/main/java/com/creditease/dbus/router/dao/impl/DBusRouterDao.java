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

package com.creditease.dbus.router.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.creditease.dbus.encoders.EncodePlugin;
import com.creditease.dbus.router.bean.FixColumnOutPutMeta;
import com.creditease.dbus.router.bean.Resources;
import com.creditease.dbus.router.bean.Sink;
import com.creditease.dbus.router.container.DataSourceContainer;
import com.creditease.dbus.router.dao.IDBusRouterDao;
import com.creditease.dbus.router.encode.DBusRouterEncodeColumn;
import com.creditease.dbus.router.util.DBUtil;
import com.creditease.dbus.router.util.DBusRouterConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mal on 2018/5/22.
 */
public class DBusRouterDao implements IDBusRouterDao {

    private static Logger logger = LoggerFactory.getLogger(DBusRouterDao.class);

    private String getQueryResourcesSql(Integer projectTopoTableId) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT                                        ");
        sql.append("    DISTINCT                                  ");
        sql.append("    tdd.`ds_name`,                            ");
        sql.append("    tdt.`schema_name`,                        ");
        sql.append("    tdt.`table_name`,                         ");
        sql.append("    tdt.`id`,                                 ");
        sql.append("    tdt.`output_topic`                        ");
        sql.append("FROM                                          ");
        sql.append("    `t_project_topo` tpt,                     ");
        sql.append("    `t_project_topo_table` tptt,              ");
        sql.append("    `t_data_tables` tdt,                      ");
        sql.append("    `t_dbus_datasource` tdd                   ");
        sql.append("WHERE                                         ");
        sql.append("    tpt.`topo_name` = ? AND                   ");
        sql.append("    tpt.`project_id` = tptt.`project_id` AND  ");
        sql.append("    tpt.`id` = tptt.`topo_id` AND             ");
        sql.append("    tptt.`table_id` = tdt.`id` AND            ");
        if (projectTopoTableId != null) {
            sql.append("    tptt.`id` = ? AND                     ");
        } else {
            sql.append("    tptt.`status` in ('running','changed') AND");
        }
        sql.append("    tdt.`ds_id` = tdd.`id`                    ");
        return sql.toString();
    }

    @Override
    public List<Resources> queryResources(String topoName) throws SQLException {
        return queryResources(topoName, null);
    }

    @Override
    public List<Resources> queryResources(String topoName, Integer projectTopoTableId) throws SQLException {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<Resources> resources = null;
        try {
            conn = DataSourceContainer.getInstance().getConn(DBusRouterConstants.DBUS_ROUTER_DB_KEY);
            ps = conn.prepareStatement(getQueryResourcesSql(projectTopoTableId));
            int idx = 1;
            ps.setString(idx++, topoName);
            if (projectTopoTableId != null)
                ps.setInt(idx++, projectTopoTableId);
            rs = ps.executeQuery();
            while (rs.next()) {
                Resources vo = new Resources();
                vo.setDsName(rs.getString("ds_name"));
                vo.setSchemaName(rs.getString("schema_name"));
                vo.setTableName(rs.getString("table_name"));
                vo.setTableId(rs.getLong("id"));
                vo.setTopicName(rs.getString("output_topic"));
                if (resources == null)
                    resources = new ArrayList<>();
                resources.add(vo);
            }
        } catch (SQLException e) {
            logger.error("load topo " + topoName + " of resources error.", e);
            throw e;
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        return resources;
    }


    private String getQuerySinksSql(Integer projectTopoTableId) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT                                      ");
        sql.append("    DISTINCT                                ");
        sql.append("    tp.`project_name`,                      ");
        sql.append("    tdd.`ds_name`,                          ");
        sql.append("    tdt.`schema_name`,                      ");
        sql.append("    tdt.`table_name`,                       ");
        sql.append("    tptt.`output_topic`,                    ");
        sql.append("    ts.`url`,                               ");
        sql.append("    ts.`sink_type`                          ");
        sql.append("FROM                                        ");
        sql.append("    `t_project_topo` tpt,                   ");
        sql.append("    `t_project` tp,                         ");
        sql.append("    `t_project_topo_table` tptt,            ");
        sql.append("    `t_data_tables` tdt,                    ");
        sql.append("    `t_dbus_datasource` tdd,                ");
        sql.append("    `t_sink` ts                             ");
        sql.append("WHERE                                       ");
        sql.append("    tpt.`topo_name` = ? AND                 ");
        sql.append("    tpt.`project_id` = tp.`id` AND          ");
        sql.append("    tpt.`project_id` = tptt.`project_id` AND");
        sql.append("    tpt.`id` = tptt.`topo_id` AND           ");
        if (projectTopoTableId != null) {
            sql.append("    tptt.`id` = ? AND                   ");
        } else {
            sql.append("    tptt.`status` in ('running','changed') AND");
        }
        sql.append("    tptt.`table_id` = tdt.`id` AND          ");
        sql.append("    tdt.`ds_id` = tdd.`id` AND              ");
        sql.append("    tptt.`sink_id` = ts.`id`                ");
        return sql.toString();
    }

    @Override
    public List<Sink> querySinks(String topoName) throws SQLException {
        return querySinks(topoName, null);
    }

    @Override
    public List<Sink> querySinks(String topoName, Integer projectTopoTableId) throws SQLException {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<Sink> sinks = null;
        try {
            conn = DataSourceContainer.getInstance().getConn(DBusRouterConstants.DBUS_ROUTER_DB_KEY);
            ps = conn.prepareStatement(getQuerySinksSql(projectTopoTableId));
            int idx = 1;
            ps.setString(idx++, topoName);
            if (projectTopoTableId != null)
                ps.setInt(idx++, projectTopoTableId);
            rs = ps.executeQuery();
            while (rs.next()) {
                Sink vo = new Sink();
                vo.setProjectName(rs.getString("project_name"));
                vo.setDsName(rs.getString("ds_name"));
                vo.setSchemaName(rs.getString("schema_name"));
                vo.setTableName(rs.getString("table_name"));
                vo.setTopic(rs.getString("output_topic"));
                vo.setUrl(rs.getString("url"));
                vo.setSinkType(rs.getString("sink_type"));
                if (sinks == null)
                    sinks = new ArrayList<>();
                sinks.add(vo);
            }
        } catch (SQLException e) {
            logger.error("load topo " + topoName + " of sinks error.", e);
            throw e;
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        return sinks;
    }

    private String getQueryEncodePluginsSql() {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT                             ");
        sql.append("    tep.`id`,                      ");
        sql.append("    tep.`name`,                    ");
        sql.append("    tep.`path`                     ");
        sql.append("FROM                               ");
        sql.append("    `t_project_topo` tpt,          ");
        sql.append("    `t_encode_plugins` tep         ");
        sql.append("WHERE                              ");
        sql.append("    tpt.`topo_name` = ?            ");
        sql.append("    AND tpt.`project_id` = tep.`project_id`");
        sql.append("    AND tep.`status` = 'active'    ");
        sql.append("UNION                              ");
        sql.append("SELECT                             ");
        sql.append("    tep.`id`,                      ");
        sql.append("    tep.`name`,                    ");
        sql.append("    tep.`path`                     ");
        sql.append("FROM                               ");
        sql.append("    `t_encode_plugins` tep         ");
        sql.append("WHERE                              ");
        sql.append("    tep.`project_id` = 0           ");
        sql.append("    AND tep.`status` = 'active'    ");
        return sql.toString();
    }

    @Override
    public List<EncodePlugin> queryEncodePlugins(String topoName) throws SQLException {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<EncodePlugin> plugins = new ArrayList<>();
        try {
            conn = DataSourceContainer.getInstance().getConn(DBusRouterConstants.DBUS_ROUTER_DB_KEY);
            ps = conn.prepareStatement(getQueryEncodePluginsSql());
            ps.setString(1, topoName);
            rs = ps.executeQuery();
            while (rs.next()) {
                EncodePlugin vo = new EncodePlugin();
                vo.setId(rs.getString("id"));
                vo.setName(rs.getString("name"));
                vo.setJarPath(rs.getString("path"));
                plugins.add(vo);
            }
        } catch (SQLException e) {
            logger.error("load topo " + topoName + " of encode plugins error.", e);
            throw e;
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        return plugins;
    }

    public String getQueryEncodeConfigSql(Integer projectTopoTableId) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT                                                   ");
        sql.append("    tptt.`table_id`,                                     ");
        sql.append("    tptt.`id` tptt_id,                                   ");
        sql.append("    tptteoc.`encode_plugin_id`,                          ");
        sql.append("    tptteoc.`encode_type`,                               ");
        sql.append("    tptteoc.`field_name`,                                ");
        sql.append("    tptteoc.`field_type`,                                ");
        sql.append("    tptteoc.`data_length`,                               ");
        sql.append("    tptteoc.`data_precision`,                            ");
        sql.append("    tptteoc.`data_scale`,                                ");
        sql.append("    tptteoc.`encode_param`,                              ");
        sql.append("    tptteoc.`truncate`,                                  ");
        sql.append("    tptteoc.`schema_change_flag`,                        ");
        sql.append("    tptteoc.`id` tptteoc_id,                             ");
        sql.append("    tptteoc.`desc_`                                      ");
        sql.append("FROM                                                     ");
        sql.append("    `t_project_topo` tpt,                                ");
        sql.append("    `t_project_topo_table` tptt,                         ");
        sql.append("    `t_project_topo_table_encode_output_columns` tptteoc ");
        sql.append("WHERE                                                    ");
        sql.append("    tpt.`topo_name` = ?                                  ");
        sql.append("    AND tpt.`id` = tptt.`topo_id`                        ");
        sql.append("    AND tpt.`project_id` = tptt.`project_id`             ");
        sql.append("    AND tptt.`id` = tptteoc.`project_topo_table_id`      ");
        sql.append("    AND tptteoc.`special_approve` = 0                    ");
        if (projectTopoTableId != null) {
            sql.append("    AND tptt.`id` = ?                                ");
        } else {
            sql.append("    AND tptt.`status` in ('running','changed')       ");
        }
        //sql.append("    AND tptteoc.`encode_source` in (0, 1, 2)           ");

        return sql.toString();
    }

    @Override
    public Map<Long, List<DBusRouterEncodeColumn>> queryEncodeConfig(String topoName, Integer projectTopoTableId) throws SQLException {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        Map<Long, List<DBusRouterEncodeColumn>> encodesMap = null;
        try {
            conn = DataSourceContainer.getInstance().getConn(DBusRouterConstants.DBUS_ROUTER_DB_KEY);
            ps = conn.prepareStatement(getQueryEncodeConfigSql(projectTopoTableId));
            int idx = 1;
            ps.setString(idx++, topoName);
            if (projectTopoTableId != null)
                ps.setInt(idx++, projectTopoTableId);
            rs = ps.executeQuery();
            while (rs.next()) {
                DBusRouterEncodeColumn vo = new DBusRouterEncodeColumn();
                vo.setTableId(rs.getLong("table_id"));
                vo.setPluginId(rs.getLong("encode_plugin_id"));
                vo.setEncodeType(rs.getString("encode_type"));
                vo.setFieldName(rs.getString("field_name"));
                vo.setFieldType(rs.getString("field_type"));
                vo.setLength(rs.getInt("data_length"));
                vo.setPrecision(rs.getInt("data_precision"));
                vo.setScale(rs.getInt("data_scale"));
                vo.setEncodeParam(rs.getString("encode_param"));
                vo.setTruncate(rs.getBoolean("truncate"));
                vo.setDesc(rs.getString("desc_"));
                vo.setSchemaChangeFlag(rs.getInt("schema_change_flag"));
                vo.setTptteocId(rs.getLong("tptteoc_id"));
                vo.setTpttId(rs.getLong("tptt_id"));
                if (encodesMap == null)
                    encodesMap = new HashMap<>();
                if (!encodesMap.containsKey(vo.getTableId())) {
                    List<DBusRouterEncodeColumn> encodes = new ArrayList<>();
                    encodes.add(vo);
                    encodesMap.put(vo.getTableId(), encodes);
                } else {
                    encodesMap.get(vo.getTableId()).add(vo);
                }
            }
        } catch (SQLException e) {
            logger.error("load topo " + topoName + " of encode config error.", e);
            throw e;
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        return encodesMap;
    }

    @Override
    public Map<Long, List<DBusRouterEncodeColumn>> queryEncodeConfig(String topoName) throws SQLException {
        return queryEncodeConfig(topoName, null);
    }

    public String getQueryFixColumnOutPutMetaSql(Integer projectTopoTableId) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT                                         ");
        sql.append("    tptt.`meta_ver`,                           ");
        sql.append("    tptt.`id` tptt_id,                          ");
        sql.append("    tpttmv.`project_id`,                       ");
        sql.append("    tpttmv.`table_id`,                         ");
        sql.append("    tpttmv.`column_name`,                      ");
        sql.append("    tpttmv.`data_type`,                        ");
        sql.append("    tpttmv.`data_length`,                      ");
        sql.append("    tpttmv.`data_precision`,                   ");
        sql.append("    tpttmv.`data_scale`,                       ");
        sql.append("    tpttmv.`id` tpttmv_id,                     ");
        sql.append("    tpttmv.`schema_change_flag`                ");
        sql.append("FROM                                           ");
        sql.append("    `t_project_topo` tpt,                      ");
        sql.append("    `t_project_topo_table` tptt,               ");
        sql.append("    `t_project_topo_table_meta_version` tpttmv ");
        sql.append("WHERE                                          ");
        sql.append("    tpt.`topo_name` = ?                        ");
        sql.append("    AND tpt.`project_id` = tptt.`project_id`   ");
        sql.append("    AND tptt.`output_list_type` = 1            ");
        if (projectTopoTableId != null) {
            sql.append("    AND tptt.`id` = ?                      ");
        } else {
            sql.append("    AND tptt.`status` in ('running','changed') ");
        }
        sql.append("    AND tptt.`project_id` = tpttmv.`project_id`");
        sql.append("    AND tptt.`table_id` = tpttmv.`table_id`    ");
        sql.append("    AND tptt.`meta_ver` = tpttmv.`version`     ");
        return sql.toString();
    }

    @Override
    public Map<Long, List<FixColumnOutPutMeta>> queryFixColumnOutPutMeta(String topoName, Integer projectTopoTableId) throws SQLException {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        Map<Long, List<FixColumnOutPutMeta>> metaMap = null;
        try {
            conn = DataSourceContainer.getInstance().getConn(DBusRouterConstants.DBUS_ROUTER_DB_KEY);
            ps = conn.prepareStatement(getQueryFixColumnOutPutMetaSql(projectTopoTableId));
            int idx = 1;
            ps.setString(idx++, topoName);
            if (projectTopoTableId != null)
                ps.setInt(idx++, projectTopoTableId);
            rs = ps.executeQuery();
            while (rs.next()) {
                FixColumnOutPutMeta vo = new FixColumnOutPutMeta();
                vo.setVersion(rs.getLong("meta_ver"));
                vo.setTpttId(rs.getLong("tptt_id"));
                vo.setProjectId(rs.getLong("project_id"));
                vo.setTableId(rs.getLong("table_id"));
                vo.setColumnName(rs.getString("column_name"));
                vo.setDataType(rs.getString("data_type"));
                vo.setLength(rs.getInt("data_length"));
                vo.setPrecision(rs.getInt("data_precision"));
                vo.setScale(rs.getInt("data_scale"));
                vo.setSchemaChangeFlag(rs.getInt("schema_change_flag"));
                vo.setTpttmvId(rs.getLong("tpttmv_id"));
                if (metaMap == null)
                    metaMap = new HashMap<>();
                if (!metaMap.containsKey(vo.getTableId())) {
                    List<FixColumnOutPutMeta> metaVos = new ArrayList<>();
                    metaVos.add(vo);
                    metaMap.put(vo.getTableId(), metaVos);
                } else {
                    metaMap.get(vo.getTableId()).add(vo);
                }
            }
        } catch (SQLException e) {
            logger.error("load topo " + topoName + " of fix column out put meta error.", e);
            throw e;
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        return metaMap;
    }

    @Override
    public Map<Long, List<FixColumnOutPutMeta>> queryFixColumnOutPutMeta(String topoName) throws SQLException {
        return queryFixColumnOutPutMeta(topoName, null);
    }

    private String getToggleProjectTopologyTableStatusSql() {
        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE                     ");
        sql.append("    `t_project_topo_table` ");
        sql.append("SET                        ");
        sql.append("    `status` = ?           ");
        sql.append("WHERE                      ");
        sql.append("    `id` = ?               ");
        return sql.toString();
    }

    @Override
    public int toggleProjectTopologyTableStatus(Integer id, String status) throws SQLException {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        int ret = 0;
        try {
            conn = DataSourceContainer.getInstance().getConn(DBusRouterConstants.DBUS_ROUTER_DB_KEY);
            ps = conn.prepareStatement(getToggleProjectTopologyTableStatusSql());
            int idx = 1;
            ps.setString(idx++, status);
            ps.setInt(idx++, id);
            ret = ps.executeUpdate();
        } catch (SQLException e) {
            logger.error("update project topology table status error.", e);
            throw e;
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        return ret;
    }

    private String getToggleProjectTopologyStatusSql() {
        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE               ");
        sql.append("    `t_project_topo` ");
        sql.append("SET                  ");
        sql.append("    `status` = ?     ");
        sql.append("WHERE                ");
        sql.append("    `id` = ?         ");
        return sql.toString();
    }

    @Override
    public int toggleProjectTopologyStatus(Integer id, String status) throws SQLException {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        int ret = 0;
        try {
            conn = DataSourceContainer.getInstance().getConn(DBusRouterConstants.DBUS_ROUTER_DB_KEY);
            ps = conn.prepareStatement(getToggleProjectTopologyStatusSql());
            int idx = 1;
            ps.setString(idx++, status);
            ps.setInt(idx++, id);
            ret = ps.executeUpdate();
        } catch (SQLException e) {
            logger.error("update project topology status error.", e);
            throw e;
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        return ret;
    }

    private String getUpdateProjectTopologyConfigSql() {
        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE                ");
        sql.append("    `t_project_topo`  ");
        sql.append("SET                   ");
        sql.append("    `topo_config` = ? ");
        sql.append("WHERE                 ");
        sql.append("    `id` = ?          ");
        return sql.toString();
    }

    @Override
    public int updateProjectTopologyConfig(Integer id, String config) throws SQLException {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        int ret = 0;
        try {
            conn = DataSourceContainer.getInstance().getConn(DBusRouterConstants.DBUS_ROUTER_DB_KEY);
            ps = conn.prepareStatement(getUpdateProjectTopologyConfigSql());
            int idx = 1;
            ps.setString(idx++, config);
            ps.setInt(idx++, id);
            ret = ps.executeUpdate();
        } catch (SQLException e) {
            logger.error("update project topology config error.", e);
            throw e;
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        return ret;
    }

    private String getIsUsingTopicSql() {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT                                       ");
        sql.append("  COUNT(tptt.`id`) cnt                       ");
        sql.append("FROM                                         ");
        sql.append("  `t_project_topo_table` tptt,               ");
        sql.append("  (SELECT                                    ");
        sql.append("     *                                       ");
        sql.append("   FROM                                      ");
        sql.append("     `t_project_topo_table` t                ");
        sql.append("   WHERE                                     ");
        sql.append("     t.`id` = ?) wk                          ");
        sql.append("WHERE                                        ");
        sql.append("  tptt.`status` in ('running', 'changed') AND");
        sql.append("  tptt.`project_id` = wk.`project_id` AND    ");
        sql.append("  tptt.`output_topic` = wk.`output_topic` AND");
        sql.append("  tptt.`topo_id` = wk.`topo_id` AND          ");
        sql.append("  tptt.`table_id` != wk.`table_id`           ");
        return sql.toString();
    }

    @Override
    public boolean isUsingTopic(Integer id) throws SQLException {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean isUsing = false;
        try {
            conn = DataSourceContainer.getInstance().getConn(DBusRouterConstants.DBUS_ROUTER_DB_KEY);
            ps = conn.prepareStatement(getIsUsingTopicSql());
            int idx = 1;
            ps.setInt(idx++, id);
            rs = ps.executeQuery();
            if (rs.next())
                isUsing = (rs.getInt("cnt") > 0);
        } catch (SQLException e) {
            logger.error("when stop topology table judgment topic is can removed error.", e);
            throw e;
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        return isUsing;
    }

    private String getUpdateTpttSchemaChangeSql() {
        StringBuilder sql = new StringBuilder();
        sql.append(" UPDATE ");
        sql.append("     t_project_topo_table ");
        sql.append(" SET ");
        sql.append("    `schema_change_flag` = ? ");
        sql.append(" WHERE ");
        sql.append("    `id` = ? ");
        return sql.toString();
    }

    @Override
    public int updateTpttSchemaChange(Long id, int value) throws SQLException {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        int ret = 0;
        try {
            conn = DataSourceContainer.getInstance().getConn(DBusRouterConstants.DBUS_ROUTER_DB_KEY);
            ps = conn.prepareStatement(getUpdateTpttSchemaChangeSql());
            int idx = 1;
            ps.setInt(idx++, value);
            ps.setLong(idx++, id);
            ret = ps.executeUpdate();
        } catch (SQLException e) {
            logger.error("update tptt schema change flag error.", e);
            throw e;
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        return ret;
    }

    private String getUpdateTpttmvSchemaChangeSql() {
        StringBuilder sql = new StringBuilder();
        sql.append(" UPDATE ");
        sql.append("     t_project_topo_table_meta_version ");
        sql.append(" SET ");
        sql.append("    `schema_change_flag` = ? ");
        sql.append("   ,`schema_change_comment` = CONCAT(`schema_change_comment`, ',', ?) ");
        // sql.append("     ,`schema_change_comment` = if (ifnull(`schema_change_comment`, 1) = 1, ?, CONCAT(`schema_change_comment`, ',', ?)) ");
        sql.append(" WHERE ");
        sql.append("    `id` = ? ");
        return sql.toString();
    }

    @Override
    public int updateTpttmvSchemaChange(Long id, int value, String changeComment) throws SQLException {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        int ret = 0;
        try {
            conn = DataSourceContainer.getInstance().getConn(DBusRouterConstants.DBUS_ROUTER_DB_KEY);
            ps = conn.prepareStatement(getUpdateTpttmvSchemaChangeSql());
            int idx = 1;
            ps.setInt(idx++, value);
            ps.setString(idx++, changeComment);
            ps.setLong(idx++, id);
            ret = ps.executeUpdate();
        } catch (SQLException e) {
            logger.error("update tpttmv schema change flag error.", e);
            throw e;
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        return ret;
    }

    // update actor set col_1 = if (ifnull(col_1, 1) = 1 , 'a', CONCAT(col_1, ',', 'a')) where actor_id = 8944;
    private String getUpdateTptteocSchemaChangeSql() {
        StringBuilder sql = new StringBuilder();
        sql.append(" UPDATE ");
        sql.append("     t_project_topo_table_encode_output_columns ");
        sql.append(" SET ");
        sql.append("    `schema_change_flag` = ? ");
        sql.append("   ,`schema_change_comment` = CONCAT(`schema_change_comment`, ',', ?) ");
        // sql.append("    ,`schema_change_comment` = if (ifnull(`schema_change_comment`, 1) = 1, ?, CONCAT(`schema_change_comment`, ',', ?)) ");
        sql.append(" WHERE ");
        sql.append("    `id` = ? ");
        return sql.toString();
    }

    @Override
    public int updateTptteocSchemaChange(Long id, int value, String changeComment) throws SQLException {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        int ret = 0;
        try {
            conn = DataSourceContainer.getInstance().getConn(DBusRouterConstants.DBUS_ROUTER_DB_KEY);
            ps = conn.prepareStatement(getUpdateTptteocSchemaChangeSql());
            int idx = 1;
            ps.setInt(idx++, value);
            ps.setString(idx++, changeComment);
            ps.setLong(idx++, id);
            ret = ps.executeUpdate();
        } catch (SQLException e) {
            logger.error("update tptteoc schema change flag error.", e);
            throw e;
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        return ret;
    }

}
