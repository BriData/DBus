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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.heartbeat.container.DataSourceContainer;
import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.dao.ILoadDbusConfigDao;
import com.creditease.dbus.heartbeat.log.LoggerFactory;
import com.creditease.dbus.heartbeat.util.DBUtil;
import com.creditease.dbus.heartbeat.vo.DsVo;
import com.creditease.dbus.heartbeat.vo.MonitorNodeVo;
import com.creditease.dbus.heartbeat.vo.ProjectMonitorNodeVo;
import com.creditease.dbus.heartbeat.vo.ProjectNotifyEmailsVO;
import com.creditease.dbus.heartbeat.vo.TargetTopicVo;

import org.apache.commons.lang.StringUtils;

public class LoadDbusConfigDaoImpl implements ILoadDbusConfigDao {

    private String getQuerySidConfigSql(String dsName) {
        StringBuilder sql = new StringBuilder();
        sql.append(" select ");
        sql.append("     ds_name,");
        sql.append("     ds_type,");
        sql.append("     ds_desc,");
        sql.append("     master_url,");
        sql.append("     slave_url,");
        sql.append("     dbus_user,");
        sql.append("     dbus_pwd,");
        sql.append("     IFNULL(ds_partition, '0') ds_partition,");
        sql.append("     ctrl_topic");
        sql.append(" from ");
        sql.append("     t_dbus_datasource ");
        sql.append(" where ");
        sql.append("     status = 'active' ");
        if (StringUtils.isNotBlank(dsName)) {
            sql.append(" and ds_name not in (" + dsName + ")");
        }
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
            ps = conn.prepareStatement(getQuerySidConfigSql(HeartBeatConfigContainer.getInstance().getHbConf().getExcludeSchema()));
            // ps.setString(1, HeartBeatConfigContainer.getInstance().getHbConf().getExcludeSchema());
            rs = ps.executeQuery();
            while (rs.next()) {
                DsVo vo = new DsVo();
                vo.setUrl(rs.getString("master_url"));
                vo.setSlvaeUrl(rs.getString("slave_url"));
                vo.setUserName(rs.getString("dbus_user"));
                vo.setPassword(rs.getString("dbus_pwd"));
                vo.setType(rs.getString("ds_type"));
                vo.setKey(rs.getString("ds_name"));
                vo.setDsPartition(rs.getString("ds_partition"));
                vo.setCtrlTopic(rs.getString("ctrl_topic"));
                if (DbusDatasourceType.stringEqual(vo.getType(), DbusDatasourceType.ORACLE)) {
                    vo.setDriverClass(DbusDatasourceType.getDataBaseDriverClass(DbusDatasourceType.ORACLE));
                } else if (DbusDatasourceType.stringEqual(vo.getType(), DbusDatasourceType.MYSQL)) {
                    vo.setDriverClass(DbusDatasourceType.getDataBaseDriverClass(DbusDatasourceType.MYSQL));
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

    private String getQueryMonitorNodeSql(String dsName) {
        StringBuilder sql = new StringBuilder();
        sql.append(" select ");
        sql.append("     dbus.ds_name,");
        sql.append("     IFNULL(dbus.ds_partition, '0') ds_partition,");
        sql.append("     tds.schema_name,");
        sql.append("     tdt.table_name");
        sql.append(" from ");
        sql.append("     t_dbus_datasource dbus, ");
        sql.append("     t_data_schema tds, ");
        sql.append("     t_data_tables tdt ");
        sql.append(" where ");
        sql.append("     dbus.id = tds.ds_id");
        sql.append("     and dbus.status = 'active'");
        if (StringUtils.isNotBlank(dsName)) {
//        sql.append("     and tds.schema_name not in (?)");
            sql.append(" and dbus.ds_name not in (" + dsName + ")");
        }
        sql.append("     and tds.status = 'active'");
        sql.append("     and tds.id = tdt.schema_id");
        sql.append("     and tdt.status <> 'inactive'");
        sql.append(" order by");
        sql.append("     ds_name, schema_name, table_name");
        return sql.toString();
    }

    /**
     * 查询projectTopoTable中表的信息（也就是需要监控的节点）
     */
    private String getQueryProjectMonitorNodeSql() {
        StringBuilder sql = new StringBuilder();
        sql.append(" select ");
        sql.append("     project.project_name,");
        sql.append("     projectTopo.topo_name,");
        sql.append("     datasource.ds_name,");
        sql.append("     dataSchema.schema_name,");
        sql.append("     dataTable.table_name");
        sql.append(" from ");
        sql.append("     t_project_topo_table projectTable, ");
        sql.append("     t_project project, ");
        sql.append("     t_data_tables dataTable, ");
        sql.append("     t_data_schema dataSchema, ");
        sql.append("     t_dbus_datasource datasource, ");
        sql.append("     t_project_topo projectTopo ");
        sql.append(" where ");
        sql.append("     projectTable.status!='stopped' and");
        sql.append("     projectTable.project_id=project.id and");
        sql.append("     projectTable.topo_id=projectTopo.id and");
        sql.append("     projectTable.table_id=dataTable.id and");
        sql.append("     dataTable.ds_id=datasource.id and");
        sql.append("     dataTable.schema_id=dataSchema.id");
        sql.append(" order by");
        sql.append("     project_name, topo_name,ds_name, schema_name, table_name");
        return sql.toString();
    }

    @Override
    public Set<ProjectMonitorNodeVo> queryProjectMonitorNode(String key) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        Set<ProjectMonitorNodeVo> list = new LinkedHashSet<>();//最后返回的结果
        try {
            conn = DataSourceContainer.getInstance().getConn(key);//获得数据库连接
            ps = conn.prepareStatement(getQueryProjectMonitorNodeSql());
            ps.setString(1, HeartBeatConfigContainer.getInstance().getHbConf().getExcludeSchema());
            rs = ps.executeQuery();
            while (rs.next()) {
                ProjectMonitorNodeVo projectMonitorNodeVo = new ProjectMonitorNodeVo();
                projectMonitorNodeVo.setProjectName(rs.getString("project_name"));
                projectMonitorNodeVo.setTopoName(rs.getString("topo_name"));
                projectMonitorNodeVo.setDsName(rs.getString("ds_name"));
                projectMonitorNodeVo.setSchema(rs.getString("schema_name"));
                projectMonitorNodeVo.setTableName(rs.getString("table_name"));
                //将查询结果放入结果集
                list.add(projectMonitorNodeVo);
            }
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[db-LoadDbusConfigDao]", e);
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        LoggerFactory.getLogger().info("[db-LoadDbusConfigDao] key: " + key + ", table数量 " + list.size());
        return list;
    }

    /**
     * 构造查询通知email的sql语句
     */
    private String getQueryNotifyEmailsSql(){
        /*
         SELECT
         schema_change_notify_flag,
         schema_change_notify_emails,
         slave_sync_delay_notify_flag,
         slave_sync_delay_notify_emails,
         fullpull_notify_flag,
         fullpull_notify_emails,
         data_delay_notify_flag,
         data_delay_notify_emails
         FROM`t_project`
         WHERE project_name='dbus-wsn-test'
         */
        StringBuilder sql = new StringBuilder();
        sql.append(" select");
        sql.append("       schema_change_notify_flag,");
        sql.append("       schema_change_notify_emails,");
        sql.append("       slave_sync_delay_notify_flag,");
        sql.append("       slave_sync_delay_notify_emails,");
        sql.append("       fullpull_notify_flag,");
        sql.append("       fullpull_notify_emails,");
        sql.append("       data_delay_notify_flag,");
        sql.append("       data_delay_notify_emails");
        sql.append(" from");
        sql.append("       t_project");
        sql.append(" where");
        sql.append("       project_name=? ");
        return sql.toString();
    }

    @Override
    public ProjectNotifyEmailsVO queryNotifyEmails(String key, String projectName) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        ProjectNotifyEmailsVO emailsVO = new ProjectNotifyEmailsVO();//最后返回的结果
        try {
            conn = DataSourceContainer.getInstance().getConn(key);//获得数据库连接
            ps = conn.prepareStatement(getQueryNotifyEmailsSql());
            ps.setString(1,projectName);
            //ps.setString(1, HeartBeatConfigContainer.getInstance().getHbConf().getExcludeSchema());
            Set<String> excludeSchema = getExcludeDbSchema(HeartBeatConfigContainer.getInstance().getHbConf().getExcludeSchema());
            rs = ps.executeQuery();
            while (rs.next()) {
                int schemaChangeNotifyFlag =Integer.valueOf(rs.getString("schema_change_notify_flag"));
                String schemaChangeNotifyEmailsStr =rs.getString("schema_change_notify_emails");
                int slaveSyncDelayNotifyFlag=Integer.valueOf(rs.getString("slave_sync_delay_notify_flag"));
                String slaveSyncDelayNotifyEmailsStr = rs.getString("slave_sync_delay_notify_emails");
                int fullpullNotifyFlag = Integer.valueOf(rs.getString("fullpull_notify_flag"));
                String fullpullNotifyEmailsStr = rs.getString("fullpull_notify_emails");
                int dataDelayNotifyFlag =Integer.valueOf(rs.getString("data_delay_notify_flag"));
                String dataDelayNotifyEmails = rs.getString("data_delay_notify_emails");

                //根据flag判断mails的赋值:数组或是null
                emailsVO.setSchemaChangeEmails(
                        schemaChangeNotifyFlag == 1 ? StringUtils.split(schemaChangeNotifyEmailsStr, ",") : null
                );
                emailsVO.setMasterSlaveDelayEmails(
                        slaveSyncDelayNotifyFlag == 1 ? StringUtils.split(slaveSyncDelayNotifyEmailsStr, ",") : null
                );
                emailsVO.setFullPullerEmails(
                        fullpullNotifyFlag == 1 ? StringUtils.split(fullpullNotifyEmailsStr, ",") : null
                );
                emailsVO.setTopologyDelayEmails(
                        dataDelayNotifyFlag == 1 ? StringUtils.split(dataDelayNotifyEmails, ",") : null
                );
            }
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[db-queryNotifyEmails]", e);
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        LoggerFactory.getLogger().info("[db-queryNotifyEmails] key: " + key + ", projectName " + projectName);
        return emailsVO;
    }

    /**
     * 构造查询通知email的sql语句
     */
    private String getQueryRelatedNotifyEmailsSql() {
        return "select\n" +
                "  schema_change_notify_flag,\n" +
                "  schema_change_notify_emails,\n" +
                "  data_delay_notify_flag,\n" +
                "  data_delay_notify_emails,\n" +
                "  fullpull_notify_flag,\n" +
                "  fullpull_notify_emails,\n" +
                "  slave_sync_delay_notify_flag,\n" +
                "  slave_sync_delay_notify_emails\n" +
                "from t_dbus_datasource, t_data_tables, t_project_topo_table, t_project\n" +
                "where t_dbus_datasource.id = t_data_tables.ds_id\n" +
                "      and t_project_topo_table.table_id = t_data_tables.id\n" +
                "      and t_project.id = t_project_topo_table.project_id\n" +
                "      and t_dbus_datasource.ds_name = ?\n" +
                "      and t_data_tables.schema_name = ?\n" +
                "      and t_data_tables.table_name = ?";
    }

    @Override
    public List<ProjectNotifyEmailsVO> queryRelatedNotifyEmails(String key, String datasource, String schema, String table) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<ProjectNotifyEmailsVO> ret = new ArrayList<>();
        try {
            conn = DataSourceContainer.getInstance().getConn(key);//获得数据库连接
            ps = conn.prepareStatement(getQueryRelatedNotifyEmailsSql());
            ps.setString(1, datasource);
            ps.setString(2, schema);
            ps.setString(3, table);
            rs = ps.executeQuery();
            while (rs.next()) {
                ProjectNotifyEmailsVO emailsVO = new ProjectNotifyEmailsVO();
                int schemaChangeNotifyFlag = Integer.valueOf(rs.getString("schema_change_notify_flag"));
                String schemaChangeNotifyEmailsStr = rs.getString("schema_change_notify_emails");
                int slaveSyncDelayNotifyFlag = Integer.valueOf(rs.getString("slave_sync_delay_notify_flag"));
                String slaveSyncDelayNotifyEmailsStr = rs.getString("slave_sync_delay_notify_emails");
                int fullpullNotifyFlag = Integer.valueOf(rs.getString("fullpull_notify_flag"));
                String fullpullNotifyEmailsStr = rs.getString("fullpull_notify_emails");
                int dataDelayNotifyFlag = Integer.valueOf(rs.getString("data_delay_notify_flag"));
                String dataDelayNotifyEmails = rs.getString("data_delay_notify_emails");
                //根据flag判断mails的赋值:数组或是null
                emailsVO.setSchemaChangeEmails(
                        schemaChangeNotifyFlag == 1 ? StringUtils.split(schemaChangeNotifyEmailsStr, ",") : null
                );
                emailsVO.setMasterSlaveDelayEmails(
                        slaveSyncDelayNotifyFlag == 1 ? StringUtils.split(slaveSyncDelayNotifyEmailsStr, ",") : null
                );
                emailsVO.setFullPullerEmails(
                        fullpullNotifyFlag == 1 ? StringUtils.split(fullpullNotifyEmailsStr, ",") : null
                );
                emailsVO.setTopologyDelayEmails(
                        dataDelayNotifyFlag == 1 ? StringUtils.split(dataDelayNotifyEmails, ",") : null
                );
                ret.add(emailsVO);
            }
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[db-queryNotifyEmails]", e);
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
        LoggerFactory.getLogger().info("[db-queryNotifyEmails] key: " + key + ", datasource " + datasource + ", schema " + schema + ", table " + table);
        return ret;
    }

    @Override
    public Set<MonitorNodeVo> queryMonitorNode(String key) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        Set<MonitorNodeVo> list = new LinkedHashSet<>();
        try {
            conn = DataSourceContainer.getInstance().getConn(key);
            ps = conn.prepareStatement(getQueryMonitorNodeSql(HeartBeatConfigContainer.getInstance().getHbConf().getExcludeSchema()));
            // ps.setString(1, HeartBeatConfigContainer.getInstance().getHbConf().getExcludeSchema());
            Set<String> excludeSchema = getExcludeDbSchema(HeartBeatConfigContainer.getInstance().getHbConf().getExcludeSchema());
            rs = ps.executeQuery();
            while (rs.next()) {
                MonitorNodeVo vo = new MonitorNodeVo();
                vo.setDsName(rs.getString("ds_name"));
                vo.setDsPartition(rs.getString("ds_partition"));
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

    private String queryTargetTopicSql(String dsName) {
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
        if (StringUtils.isNotBlank(dsName)) {
//        sql.append("     and tds.schema_name not in (?)");
            sql.append(" and dbus.ds_name not in (" + dsName + ")");
        }
        sql.append("     and tds.status = 'active'");
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
            ps = conn.prepareStatement(queryTargetTopicSql(HeartBeatConfigContainer.getInstance().getHbConf().getExcludeSchema()));
            // ps.setString(1, HeartBeatConfigContainer.getInstance().getHbConf().getExcludeSchema());
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
