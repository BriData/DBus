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

package com.creditease.dbus.stream.common.appender.utils;

import com.alibaba.druid.util.StringUtils;
import com.creditease.dbus.commons.MetaWrapper;
import com.creditease.dbus.msgencoder.EncodeColumn;
import com.creditease.dbus.stream.common.appender.bean.*;
import com.creditease.dbus.stream.common.appender.exception.RuntimeSQLException;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.sql.Date;
import java.util.*;

/**
 * Created by Shrimp on 16/5/18.
 */
public class DBFacade {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private DataSource ds;

    private interface ResultSetConverter<T> {
        T convert(ResultSet rs) throws Exception;
    }

    public DBFacade(DataSource dataSource) {
        this.ds = dataSource;
    }

    /**
     * 获取指定表的schema信息
     *
     * @param fullName 表名
     * @param dsId
     * @return AvroSchema
     * @throws SQLException
     */
    public AvroSchema queryAvroSchema(String fullName, long dsId) throws Exception {
        String sql = "select * from t_avro_schema t where t.full_name = ? and t.ds_id = ?";
        return query(sql, new Object[]{fullName, dsId}, rs -> {
            if (rs.next()) {
                AvroSchema schema = null;
                schema = new AvroSchema();
                schema.setId(rs.getLong("id"));
                schema.setTs(rs.getTimestamp("create_time"));
                schema.setSchemaName(rs.getString("schema_name"));
                schema.setSchema(rs.getString("schema_text"));
                schema.setFullName(rs.getString("full_name"));
                schema.setSchemaHash(rs.getInt("schema_hash"));
                schema.setNamespace(rs.getString("namespace"));
                return schema;
            }
            return null;
        });
    }

    /**
     * 持久化avro schema
     *
     * @param schema
     */
    public void saveAvroSchema(AvroSchema schema) throws Exception {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = ds.getConnection();

            // 判断是否存在记录
            ps = conn.prepareStatement("select 1 from t_avro_schema t where t.full_name = ? limit 1");
            ps.setString(1, schema.getFullName());
            rs = ps.executeQuery();
            if (rs.next()) {
                return;
            }

            String sql = "insert into t_avro_schema(namespace,schema_name,full_name,schema_hash,schema_text,create_time,ds_id) values(?,?,?,?,?,?,?)";
            ps = conn.prepareStatement(sql);
            ps.setString(1, schema.getNamespace());
            ps.setString(2, schema.getSchemaName());
            ps.setString(3, schema.getFullName());
            ps.setLong(7, schema.getDsId());
            ps.setInt(4, schema.getSchemaHash());
            ps.setString(5, schema.getSchema());
            ps.setTimestamp(6, schema.getTs());

            ps.executeUpdate();
        } finally {
            close(rs, ps, conn);
        }
    }


    public void updateColumnComments(MetaVersion version, MetaWrapper.MetaCell cell) {
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = ds.getConnection();
            String sql;

            sql = "update t_table_meta set comments = ?,alter_time=? where ver_id=? and column_name=?";
            ps = conn.prepareStatement(sql);

            ps.setString(1, cell.getComments());
            ps.setTimestamp(2, cell.getDdlTime());
            ps.setLong(3, version.getId());
            ps.setString(4, cell.getColumnName());
            ps.executeUpdate();

        } catch (Exception e) {
            logger.error("update column comments error", e);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public void updateTableComments(MetaVersion metaVersion, TableComments tableComments) {
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = ds.getConnection();
            String sql;

            sql = "update t_meta_version set comments = ? where id=?";
            ps = conn.prepareStatement(sql);

            ps.setString(1, tableComments.getComments());
            ps.setLong(2, metaVersion.getId());
            ps.executeUpdate();

        } catch (Exception e) {
            logger.error("update table comments error", e);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void insertDdlEvent(DdlEvent ddlEvent) {
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = ds.getConnection();
            String sql;

            sql = "insert into t_ddl_event(event_id,ds_id,schema_name,table_name,column_name,ver_id,trigger_ver,ddl_type,ddl) values(?,?,?,?,?,?,?,?,?)";
            ps = conn.prepareStatement(sql);

            ps.setLong(1, ddlEvent.getEventId());
            ps.setLong(2, ddlEvent.getDsId());
            ps.setString(3, ddlEvent.getSchemaName());
            ps.setString(4, ddlEvent.getTableName());
            ps.setString(5, ddlEvent.getColumnName());
            ps.setLong(6, ddlEvent.getVerId());
            ps.setString(7, ddlEvent.getTriggerVer());
            ps.setString(8, ddlEvent.getDdlType());
            ps.setString(9, ddlEvent.getDdl());

            ps.executeUpdate();

        } catch (Exception e) {
            logger.error("insert into t_ddl_event error");
            e.printStackTrace();
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 保存新的version
     *
     * @param version
     * @return
     * @throws Exception
     */
    public void createMetaVersion(MetaVersion version) throws Exception {
        Connection conn = null;
        PreparedStatement ps = null;

        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            String sql;

            sql = "insert into t_meta_version(db_name,schema_name,table_name,version,inner_version,update_time,ds_id,event_offset, event_pos,table_id,comments) values(?,?,?,?,?,?,?,?,?,?,?)";
            // 生成version信息
            ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);

            ps.setString(1, Utils.getDatasource().getDsName());
            ps.setString(2, version.getSchema());
            ps.setString(3, version.getTable());
            ps.setInt(4, version.getVersion());
            ps.setInt(5, version.getInnerVersion());
            ps.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
            ps.setLong(7, Utils.getDatasource().getId());
            ps.setLong(8, version.getOffset());
            ps.setLong(9, version.getTrailPos());
            ps.setLong(10, version.getTableId());
            ps.setString(11, version.getComments());
            ps.executeUpdate();

            // 获取自动生成的ID
            ResultSet idSet = ps.getGeneratedKeys();
            idSet.next();
            long verId = idSet.getLong("generated_key");

            // 更新version
            version.setId(verId);

            if (version.getMeta() != null && !version.getMeta().isEmpty()) {
                // 生成meta信息
                sql = "insert into t_table_meta(ver_id,column_name,column_id,original_ser,data_type,data_length,data_precision," +
                        "data_scale,nullable,is_pk,pk_position,alter_time,char_length,char_used,internal_column_id, " +
                        "hidden_column, virtual_column, original_column_name, comments, default_value) " +
                        "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

                ps = conn.prepareStatement(sql);
                List<MetaWrapper.MetaCell> columns = version.getMeta().getColumns();
                for (MetaWrapper.MetaCell column : columns) {
                    ps.setLong(1, verId);
                    ps.setString(2, column.getColumnName());
                    ps.setInt(3, column.getColumnId());
                    ps.setInt(4, version.getInnerVersion());
                    ps.setString(5, column.getDataType());
                    ps.setLong(6, column.getDataLength());
                    ps.setInt(7, column.getDataPrecision());
                    ps.setInt(8, column.getDataScale());
                    ps.setString(9, column.getNullAble());
                    ps.setString(10, column.getIspk());
                    ps.setInt(11, column.getPkPosition());
                    ps.setTimestamp(12, column.getDdlTime());
                    ps.setInt(13, column.getCharLength());
                    ps.setString(14, column.getCharUsed());
                    ps.setInt(15, column.getInternalColumnId());
                    ps.setString(16, column.getHiddenColumn());
                    ps.setString(17, column.getVirtualColumn());
                    ps.setString(18, column.getOriginalColumnName());
                    ps.setString(19, column.getComments());
                    ps.setString(20, column.getDefaultValue());
                    ps.addBatch();
                }
                ps.executeBatch();
            }

            updateTableVerHistoryNotice(conn, version.getTableId());

            ps = conn.prepareStatement("update t_data_tables set ver_id = ? where id = ?");
            ps.setLong(1, verId);
            ps.setLong(2, version.getTableId());
            ps.executeUpdate();

            conn.commit();

        } catch (Exception e) {
            if (conn != null) conn.rollback();
            logger.error("create meta and version error", e);
            throw e;
        } finally {
            if (ps != null) {
                ps.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }

    /**
     * mysql使用该函数，暂时不处理默认值问题
     *
     * @param version
     * @throws Exception
     */
    public void deleteAndInsertTableMeta(MetaVersion version) throws Exception {
        Connection conn = null;
        PreparedStatement ps = null;

        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            String sql;

            sql = "delete from t_table_meta where ver_id = ?";

            ps = conn.prepareStatement(sql);
            ps.setLong(1, version.getId());
            ps.addBatch();

            if (version.getMeta() != null && !version.getMeta().isEmpty()) {
                // 生成meta信息
                sql = "insert into t_table_meta(ver_id,column_name,column_id,original_ser,data_type,data_length,data_precision," +
                        "data_scale,nullable,is_pk,pk_position,alter_time,char_length,char_used,internal_column_id, " +
                        "hidden_column, virtual_column, original_column_name, comments) " +
                        "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

                ps = conn.prepareStatement(sql);
                List<MetaWrapper.MetaCell> columns = version.getMeta().getColumns();
                for (MetaWrapper.MetaCell column : columns) {
                    ps.setLong(1, version.getId());
                    ps.setString(2, column.getColumnName());
                    ps.setInt(3, column.getColumnId());
                    ps.setInt(4, version.getInnerVersion());
                    ps.setString(5, column.getDataType());
                    ps.setLong(6, column.getDataLength());
                    ps.setInt(7, column.getDataPrecision());
                    ps.setInt(8, column.getDataScale());
                    ps.setString(9, column.getNullAble());
                    ps.setString(10, column.getIspk());
                    ps.setInt(11, column.getPkPosition());
                    ps.setTimestamp(12, column.getDdlTime());
                    ps.setInt(13, column.getCharLength());
                    ps.setString(14, column.getCharUsed());
                    ps.setInt(15, column.getInternalColumnId());
                    ps.setString(16, column.getHiddenColumn());
                    ps.setString(17, column.getVirtualColumn());
                    ps.setString(18, column.getOriginalColumnName());
                    ps.setString(19, column.getComments());
                    ps.addBatch();
                }
            }

            ps.executeBatch();
            conn.commit();

        } catch (Exception e) {
            if (conn != null) conn.rollback();
            logger.error("delete and create meta error", e);
            throw e;
        } finally {
            if (ps != null) {
                ps.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }

    /**
     * 根据数据源名称获取数据源的相关信息
     *
     * @param datasource 数据源名
     * @return 完整的数据源信息
     * @throws SQLException
     */
    public Map<String, Object> queryDatasource(String datasource) throws SQLException {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = ds.getConnection();

            ps = conn.prepareStatement("select * from t_dbus_datasource t where t.ds_name = ? limit 1");
            ps.setString(1, datasource);
            rs = ps.executeQuery();
            Map<String, Object> map = null;
            if (rs.next()) {
                map = convertResultSet(rs);
            }
            return map;
        } finally {
            close(rs, ps, conn);
        }
    }

    public MetaWrapper queryMeta(long versionId) {

        String sql = "select * from  t_table_meta t where t.ver_id=? order by t.column_id asc,t.column_name asc";
        return query(sql, new Object[]{versionId}, rs -> {
            MetaWrapper mw = new MetaWrapper();
            while (rs.next()) {
                MetaWrapper.MetaCell cell = new MetaWrapper.MetaCell();

                //cell.setOwner(rs.getString("owner"));
                //cell.setTableName(rs.getString("table_name"));
                cell.setColumnName(rs.getString("column_name"));
                cell.setOriginalColumnName(rs.getString("original_column_name"));
                cell.setColumnId(rs.getInt("column_id"));
                cell.setVersion(rs.getInt("original_ser"));
                cell.setDataType(rs.getString("data_type"));
                cell.setDataLength(rs.getLong("data_length"));
                cell.setDataPrecision(rs.getInt("data_precision"));
                cell.setDataScale(rs.getInt("data_scale"));
                cell.setNullAble(rs.getString("nullable"));
                cell.setDdlTime(rs.getTimestamp("alter_time"));
                cell.setIsPk(rs.getString("is_pk"));
                cell.setPkPosition(rs.getInt("pk_position"));
                cell.setCharLength(rs.getInt("char_length"));
                cell.setCharUsed(rs.getString("char_used"));
                cell.setInternalColumnId(rs.getInt("internal_column_id"));
                cell.setHiddenColumn(rs.getString("hidden_column"));
                cell.setVirtualColumn(rs.getString("virtual_column"));
                cell.setComments(rs.getString("comments"));
                String defaultValue = rs.getString("default_value");
                // 转义\n，避免生成json时出现错误
                cell.setDefaultValue(defaultValue == null ? null : defaultValue.trim().replaceAll("\n", "\\\\n"));
                mw.addMetaCell(cell);
            }
            return mw;
        });
    }

    private Map<String, Object> convertResultSet(ResultSet rs) {
        try {
            ResultSetMetaData meta = rs.getMetaData();
            int count = meta.getColumnCount();
            Map<String, Object> map = new HashMap<>();
            for (int i = 1; i <= count; i++) {
                String name = meta.getColumnName(i);
                map.put(name, rs.getObject(name));
            }
            return map;
        } catch (SQLException e) {
            throw new RuntimeSQLException(e);
        }
    }

    private void close(AutoCloseable... ps) {
        if (ps != null && ps.length > 0) {
            for (AutoCloseable p : ps) {
                try {
                    p.close();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    private List<Map<String, Object>> query(String sql, Object... args) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = ds.getConnection();
            ps = conn.prepareStatement(sql);
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }
            rs = ps.executeQuery();
            List<Map<String, Object>> list = new ArrayList<>();
            while (rs.next()) {
                list.add(convertResultSet(rs));
            }
            return list;
        } catch (Exception e) {
            throw new RuntimeSQLException(e);
        } finally {
            close(rs, ps, conn);
        }
    }

    private Map<String, Object> queryUnique(String sql, Object... args) {
        List<Map<String, Object>> mapList = query(sql, args);
        if (!mapList.isEmpty()) return mapList.get(0);
        return null;
    }

    private void executeUpdate(String sql, Object... args) {
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = ds.getConnection();
            ps = conn.prepareStatement(sql);
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }
            ps.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeSQLException(e);
        } finally {
            close(ps, conn);
        }
    }

    private <T> T query(String sql, Object[] args, ResultSetConverter<?> converter) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = ds.getConnection();
            ps = conn.prepareStatement(sql);
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }
            rs = ps.executeQuery();
            return (T) converter.convert(rs);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeSQLException(e);
        } finally {
            close(rs, ps, conn);
        }
    }

    private Map<String, Object> queryFirstRow(String sql, Object... args) {
        List<Map<String, Object>> list = query(sql, args);
        if (list != null && !list.isEmpty()) {
            return list.get(0);
        }
        return null;
    }


    /**
     * 根据schema id 查询所有的表
     *
     * @param id TabSchema的id
     * @throws Exception
     */
    public List<DataTable> queryDataTables(long id) {
        String sql = "select * from t_data_tables t where t.schema_id = ?";
        return query(sql, new Object[]{id}, rs -> {
            List<DataTable> list = Lists.newArrayList();
            while (rs.next()) {
                DataTable t = new DataTable();
                t.setId(rs.getLong("id"));
                t.setDsId(rs.getLong("ds_id"));
                t.setSchemaId(rs.getLong("schema_id"));
                t.setSchema(rs.getString("schema_name"));
                t.setTableName(rs.getString("table_name"));
                t.setOutputTopic(rs.getString("output_topic"));
                Timestamp ts = rs.getTimestamp("create_time");
                t.setVerId(rs.getLong("ver_id"));
                t.setStatus(rs.getString("status"));
                t.setPhysicalTableRegex(rs.getString("physical_table_regex"));
                t.setMetaChangeFlg(rs.getInt("meta_change_flg"));
                t.setBatchId(rs.getInt("batch_id"));
                t.setOutputBeforeUpdateFlg(rs.getInt("output_before_update_flg"));
                if (ts != null) {
                    t.setCreateTime(new Date(ts.getTime()));
                }
                list.add(t);
            }
            return list;
        });
    }

    /**
     * 查询数据库获取DataTable对象
     *
     * @param dsId       datasource id
     * @param schemaName 数据库 schema 名
     * @param table      表名
     * @return
     */
    public DataTable queryDataTable(long dsId, String schemaName, String table) {
        String sql = "select * from t_data_tables t where t.ds_id = ? and t.schema_name = ? and table_name = ?";
        return query(sql, new Object[]{dsId, schemaName, table}, rs -> {
            while (rs.next()) {
                DataTable t = new DataTable();
                t.setId(rs.getLong("id"));
                t.setDsId(rs.getLong("ds_id"));
                t.setSchemaId(rs.getLong("schema_id"));
                t.setSchema(rs.getString("schema_name"));
                t.setTableName(rs.getString("table_name"));
                t.setOutputTopic(rs.getString("output_topic"));
                Timestamp ts = rs.getTimestamp("create_time");
                t.setVerId(rs.getLong("ver_id"));
                t.setStatus(rs.getString("status"));
                t.setPhysicalTableRegex(rs.getString("physical_table_regex"));
                t.setMetaChangeFlg(rs.getInt("meta_change_flg"));
                t.setBatchId(rs.getInt("batch_id"));
                t.setOutputBeforeUpdateFlg(rs.getInt("output_before_update_flg"));
                if (ts != null) {
                    t.setCreateTime(new Date(ts.getTime()));
                }
                return t;
            }
            return null;
        });
    }


    /**
     * 查询数据库获取DataTable对象
     *
     * @param dsId       datasource id
     * @param schemaName 数据库 schema 名
     * @param table      表名
     * @return
     */
    public DataTable queryDataTable(long dsId, String schemaName, String table, String schemaId) {
        String sql = "select * from t_data_tables t where t.ds_id = ? and t.schema_name = ? and table_name = ? and ";
        return query(sql, new Object[]{dsId, schemaName, table}, rs -> {
            while (rs.next()) {
                DataTable t = new DataTable();
                t.setId(rs.getLong("id"));
                t.setDsId(rs.getLong("ds_id"));
                t.setSchemaId(rs.getLong("schema_id"));
                t.setSchema(rs.getString("schema_name"));
                t.setTableName(rs.getString("table_name"));
                t.setOutputTopic(rs.getString("output_topic"));
                Timestamp ts = rs.getTimestamp("create_time");
                t.setVerId(rs.getLong("ver_id"));
                t.setStatus(rs.getString("status"));
                t.setPhysicalTableRegex(rs.getString("physical_table_regex"));
                t.setMetaChangeFlg(rs.getInt("meta_change_flg"));
                t.setBatchId(rs.getInt("batch_id"));
                t.setOutputBeforeUpdateFlg(rs.getInt("output_before_update_flg"));
                if (ts != null) {
                    t.setCreateTime(new Date(ts.getTime()));
                }
                return t;
            }
            return null;
        });
    }

    /**
     * mysql使用该函数，暂时不处理默认值问题
     *
     * @param verId
     * @param metaWrapper
     * @throws Exception
     */
    public void saveMeta(long verId, MetaWrapper metaWrapper) throws Exception {
        Connection conn = null;
        PreparedStatement ps = null;

        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            // 生成meta信息
            String sql = "insert into t_table_meta(ver_id,column_name,column_id,original_ser,data_type,data_length,data_precision," +
                    "data_scale,nullable,is_pk,pk_position,alter_time,char_length,char_used, internal_column_id, " +
                    "hidden_column, virtual_column, original_column_name, comments) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            ps = conn.prepareStatement(sql);

            List<MetaWrapper.MetaCell> columns = metaWrapper.getColumns();
            for (MetaWrapper.MetaCell column : columns) {
                ps.setLong(1, verId);
                ps.setString(2, column.getColumnName());
                ps.setInt(3, column.getColumnId());
                ps.setInt(4, column.getVersion());
                ps.setString(5, column.getDataType());
                ps.setLong(6, column.getDataLength());
                ps.setInt(7, column.getDataPrecision());
                ps.setInt(8, column.getDataScale());
                ps.setString(9, column.getNullAble());
                ps.setString(10, column.getIspk());
                ps.setInt(11, column.getPkPosition());
                ps.setTimestamp(12, column.getDdlTime());
                ps.setInt(13, column.getCharLength());
                ps.setString(14, column.getCharUsed());
                ps.setInt(15, column.getInternalColumnId());
                ps.setString(16, column.getHiddenColumn());
                ps.setString(17, column.getVirtualColumn());
                ps.setString(18, column.getOriginalColumnName());
                ps.setString(19, column.getComments());
                ps.addBatch();
            }
            ps.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            if (conn != null) conn.rollback();
            logger.error("create meta error", e);
            throw e;
        } finally {
            if (ps != null) {
                ps.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }


    public void updateVersionPos(MetaVersion ver) throws SQLException {
        Connection conn = null;
        PreparedStatement ps = null;

        try {
            conn = ds.getConnection();
            String sql = "update t_meta_version set event_pos=? where id=?";
            ps = conn.prepareStatement(sql);

            ps.setLong(1, ver.getTrailPos());
            ps.setLong(2, ver.getId());

            ps.executeUpdate();

        } finally {
            if (ps != null) {
                ps.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }



    public void updateVersion(MetaVersion ver) throws Exception {

        Connection conn = null;
        PreparedStatement ps = null;

        try {
            conn = ds.getConnection();
            //conn.setAutoCommit(false);
            String sql = "update t_meta_version set version=? where table_id=? and inner_version=?";
            ps = conn.prepareStatement(sql);

            ps.setInt(1, ver.getVersion());
            ps.setLong(2, ver.getTableId());
            ps.setInt(3, ver.getInnerVersion());

            ps.executeUpdate();

        } finally {
            if (ps != null) {
                ps.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }

    /**
     * 根据datasource id获取该datasource下的所有已配置的schema
     *
     * @param dsId datasource id
     * @throws Exception
     */
    public List<TabSchema> queryDataSchemas(long dsId) throws Exception {
        String sql = "select * from t_data_schema t where t.ds_id = ?";
        return query(sql, new Object[]{dsId}, rs -> {
            List<TabSchema> list = new LinkedList<>();
            while (rs.next()) {
                TabSchema s = new TabSchema();
                s.setId(rs.getLong("id"));
                s.setDsId(rs.getLong("ds_id"));
                s.setSchema(rs.getString("schema_name"));
                s.setSrcTopic(rs.getString("src_topic"));
                s.setTargetTopic(rs.getString("target_topic"));

                Timestamp ts = rs.getTimestamp("create_time");
                if (s != null) {
                    s.setCreateTime(new Date(ts.getTime()));
                }
                list.add(s);
            }
            return list;
        });
    }

    /**
     * 根据datasource id获取该datasource下的所有已配置的schema
     *
     * @param dsId datasource id
     * @throws Exception
     */
    public TabSchema queryDataSchema(long dsId, String schemaName) throws Exception {
        String sql = "select * from t_data_schema t where t.ds_id = ? and t.schema_name = ?";
        return query(sql, new Object[]{dsId, schemaName}, rs -> {
            while (rs.next()) {
                TabSchema s = new TabSchema();
                s.setId(rs.getLong("id"));
                s.setDsId(rs.getLong("ds_id"));
                s.setSchema(rs.getString("schema_name"));
                s.setSrcTopic(rs.getString("src_topic"));
                s.setTargetTopic(rs.getString("target_topic"));
                s.setStatus(rs.getString("status"));
                Timestamp ts = rs.getTimestamp("create_time");
                if (s != null) {
                    s.setCreateTime(new Date(ts.getTime()));
                }
                return s;
            }
            return null;
        });
    }

    /**
     * 查询meta version信息
     *
     * @param tableId t_data_tables 表ID
     * @param pos     接收到消息的 trail pos 值
     * @param offset  接收到消息的kafka offset
     * @return MetaVersion 对象
     */
    public MetaVersion queryMetaVersion(long tableId, long pos, long offset) {
        String sql = "select * from t_meta_version t where t.table_id=? and t.event_pos<=? order by t.event_pos desc";
        Map<String, Object> map = queryFirstRow(sql, tableId, pos);

        if (map == null) {
            return null;
        }

        MetaWrapper wrapper = queryMeta((Long) map.get("id"));
        MetaVersion ver = MetaVersion.parse(map);
        ver.setMeta(wrapper);

        return ver;
    }


    public MetaVersion querySuitableMetaVersion(long tableId, long pos) {
        String sql = "select * from t_meta_version t where t.table_id=? and t.event_pos=? order by t.event_pos desc";
        Map<String, Object> map = queryFirstRow(sql, tableId, pos);

        if (map == null) {
            return null;
        }

        MetaWrapper wrapper = queryMeta((Long) map.get("id"));
        MetaVersion ver = MetaVersion.parse(map);
        ver.setMeta(wrapper);

        return ver;
    }

    //根据table id
    public MetaVersion queryMetaVersion(long tableId, long pos) {
        String sql = "select * from t_meta_version t where t.table_id=? and t.event_pos=? order by t.event_pos desc";
        Map<String, Object> map = queryFirstRow(sql, tableId, pos);

        if (map == null) {
            return null;
        } else {
            MetaWrapper wrapper = queryMeta((Long) map.get("id"));
            MetaVersion ver = MetaVersion.parse(map);
            ver.setMeta(wrapper);

            return ver;
        }

    }


    public MetaVersion queryMetaVersionByTime(long tableId, long pos, long offset) {
        String sql = "select * from t_meta_version t where t.table_id=? and t.event_pos=? and event_offset<=? order by t.event_pos desc";
        List<Map<String, Object>> rows = query(sql, tableId, pos, offset);

        Map<String, Object> map = null;
        MetaVersion version = null;
        long lag = Long.MAX_VALUE;
        for(int i = 0; i < rows.size(); i++) {
            version = MetaVersion.parse(rows.get(i));
            if((offset - version.getOffset()) < lag ) {
                lag = offset - version.getOffset();
                map = rows.get(i);
            }
        }

        if (map == null) {
           logger.info("没有找到距离 {} 最近的版本！", offset);
            return null;
        }

        MetaWrapper wrapper = queryMeta((Long) map.get("id"));
        MetaVersion ver = MetaVersion.parse(map);
        ver.setMeta(wrapper);

        return ver;
    }


    public MetaVersion queryMetaVersionFromDB(long tableId, long pos, long offset) {
        String sql = "select * from t_meta_version t where t.table_id=? and t.event_pos=? and event_offset=? order by t.event_pos desc";
        Map<String, Object> map = queryFirstRow(sql, tableId, pos, offset);

        if (map == null) {
            return null;
        }

        MetaWrapper wrapper = queryMeta((Long) map.get("id"));
        MetaVersion ver = MetaVersion.parse(map);
        ver.setMeta(wrapper);

        return ver;
    }

    public List<Long> queryAllPos(long tableId) {
        List<Long> posLists = new ArrayList<>();
        String sql = "select DISTINCT(event_pos) from t_meta_version t where t.table_id=?";
        List<Map<String, Object>> rows = query(sql, tableId);

        for(int i = 0; i < rows.size(); i++) {
            posLists.add((Long)rows.get(i).get("event_pos"));
        }
        return posLists;
    }



    public MetaVersion queryLatestMetaVersion(long tableId) {
        String sql = "select * from t_meta_version t where t.table_id=? order by t.version desc,t.update_time desc";
        Map<String, Object> map = queryFirstRow(sql, tableId);

        if (map == null) {
            return null;
        }

        MetaWrapper wrapper = queryMeta((Long) map.get("id"));
        MetaVersion ver = MetaVersion.parse(map);
        ver.setMeta(wrapper);

        return ver;
    }

    public MetaVersion queryMetaVersion(long dsId, String schemaName, String tableName) {
        DataTable tab = queryDataTable(dsId, schemaName, tableName);
        String sql = "select * from t_meta_version t where id = ?";

        Map<String, Object> map = queryUnique(sql, tab.getVerId());
        if (map == null) {
            return null;
        }

        MetaWrapper wrapper = queryMeta((Long) map.get("id"));
        MetaVersion ver = MetaVersion.parse(map);
        ver.setMeta(wrapper);

        return ver;
    }

    public MetaVersion queryMetaVersion(long verId) {
        String sql = "select * from t_meta_version t where t.id=?";
        Map<String, Object> map = queryFirstRow(sql, verId);

        if (map == null) {
            return null;
        }

        MetaWrapper wrapper = queryMeta(verId);
        MetaVersion ver = MetaVersion.parse(map);
        ver.setMeta(wrapper);

        return ver;
    }

    /**
     * 初始化data table
     *
     * @param tableId data table id
     * @param version meta version
     */
    public void initializeDataTable(long tableId, MetaVersion version) throws Exception {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet idSet = null;
        try {
            // 初始化时可能没有ID
            String existSql = "select max(t.id) as id from t_meta_version t where t.table_id=? limit 1";
            Object[] args = new Object[]{version.getTableId()};
            long verId = query(existSql, args, rs -> {
                rs.next();
                return rs.getLong("id");
            });

            conn = ds.getConnection();
            conn.setAutoCommit(false);
            String sql;

            if (verId == 0) {
                sql = "insert into t_meta_version(db_name,schema_name,table_name,version,inner_version,update_time,ds_id,event_offset, event_pos,table_id) values(?,?,?,?,?,?,?,?,?,?)";
                // 生成version信息
                ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);

                ps.setString(1, Utils.getDatasource().getDsName());
                ps.setString(2, version.getSchema());
                ps.setString(3, version.getTable());
                ps.setInt(4, version.getVersion());
                ps.setInt(5, version.getInnerVersion());
                ps.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
                ps.setLong(7, Utils.getDatasource().getId());
                ps.setLong(8, version.getOffset());
                ps.setLong(9, version.getTrailPos());
                ps.setLong(10, version.getTableId());
                ps.executeUpdate();

                // 获取自动生成的ID
                idSet = ps.getGeneratedKeys();
                idSet.next();
                verId = idSet.getLong("generated_key");

                // 生成meta信息
                sql = "insert into t_table_meta(ver_id,column_name,column_id,original_ser,data_type,data_length,data_precision," +
                        "data_scale,nullable,is_pk,pk_position,alter_time,char_length, char_used, internal_column_id, " +
                        "hidden_column, virtual_column, original_column_name, comments, default_value) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

                ps = conn.prepareStatement(sql);

                List<MetaWrapper.MetaCell> columns = version.getMeta().getColumns();
                for (MetaWrapper.MetaCell column : columns) {
                    ps.setLong(1, verId);
                    ps.setString(2, column.getColumnName());
                    ps.setInt(3, column.getColumnId());
                    ps.setInt(4, version.getInnerVersion());
                    ps.setString(5, column.getDataType());
                    ps.setLong(6, column.getDataLength());
                    ps.setInt(7, column.getDataPrecision());
                    ps.setInt(8, column.getDataScale());
                    ps.setString(9, column.getNullAble());
                    ps.setString(10, column.getIspk());
                    ps.setInt(11, column.getPkPosition());
                    ps.setTimestamp(12, column.getDdlTime());
                    ps.setInt(13, column.getCharLength());
                    ps.setString(14, column.getCharUsed());
                    ps.setInt(15, column.getInternalColumnId());
                    ps.setString(16, column.getHiddenColumn());
                    ps.setString(17, column.getVirtualColumn());
                    ps.setString(18, column.getOriginalColumnName());
                    ps.setString(19, column.getComments());
                    ps.setString(20, column.getDefaultValue());
                    ps.addBatch();
                }
                ps.executeBatch();
            }

            updateTableVerHistoryNotice(conn, version.getTableId());

            ps = conn.prepareStatement("update t_data_tables set ver_id = ? where id = ?");
            ps.setLong(1, verId);
            ps.setLong(2, tableId);
            ps.executeUpdate();

            conn.commit();
        } catch (Exception e) {
            if (conn != null) conn.rollback();
            logger.error("create meta and version error", e);
            throw e;
        } finally {
            close(idSet, ps, conn);
        }
    }

    public void updateTableVer(Long id, long verId) {
        try {
            updateTableVerHistoryNotice(ds.getConnection(), id);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        String sql = "update t_data_tables set ver_id=? where id=?";
        executeUpdate(sql, verId, id);
    }

    // 此处version的id为新的id，应该在更新ver_id之前调用该方法
    public void updateTableVerHistoryNotice(Connection conn, long tableId) {
        try {
            PreparedStatement ps = conn.prepareStatement("select id, ver_id, ver_change_history from t_data_tables where id = ?");
            ps.setLong(1, tableId);
            ResultSet oldResultSet = ps.executeQuery();
            if (oldResultSet.next()) {
                /**
                 * 两个都为空时，更新为新版本
                 * 只有历史为空时，更新为当前版本+新版本
                 * 两个都有值，更新为当前历史+新版本
                 */

                String verChangeHistory = "";
                if (oldResultSet.getObject("ver_change_history") != null) {
                    verChangeHistory = String.valueOf(oldResultSet.getObject("ver_change_history"));
                }
                String verId = "";
                if (oldResultSet.getObject("ver_id") != null) {
                    verId = String.valueOf(oldResultSet.getObject("ver_id").toString());
                }
                if (StringUtils.isEmpty(verChangeHistory)) {
                    if (StringUtils.isEmpty(verId)) {
                        // 两个都为空，不做处理，连标记也不更新，直接返回
                        logger.info("Ignore to set ver_change_history while ver_id is null");
                        return;
                    } else {
                        // 历史为空，当前版本不为空，赋值为当前版本
                        verChangeHistory = verId;
                        logger.info("Set ver_change_history = ver_id");
                    }
                } else {
                    if (StringUtils.isEmpty(verId)) {
                        // 历史不为空，当前版本却为空，不合逻辑，历史不变
                        logger.warn("An error occurred while updating verChangeHistory. Ver_change_history is not empty, but ver_id is empty. Set ver_change_history unchanged while still show notice");
                        return;
                    } else {
                        // 两个都不为空，直接将当前版本添加进来
                        verChangeHistory = verChangeHistory + "," + verId;
                        logger.info("Set ver_change_history += ver_id");
                    }
                }
                ps = conn.prepareStatement("update t_data_tables set ver_change_history = ? , ver_change_notice_flg = 1 where id = ?");
                ps.setString(1, verChangeHistory);
                ps.setLong(2, tableId);
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("updateTableVerHistoryNotice failed，error message:{}", e.getMessage());
        }
    }


    public void updateTableStatus(long id, String status) {
        String sql = "update t_data_tables set status=? where id=?";
        executeUpdate(sql, status, id);
    }

    public List<EncodeColumn> getEncodeColumns(long tableId) {
        String sql = "select c.*, m.data_length\n" +
                "from\n" +
                "    t_encode_columns c,\n" +
                "    t_data_tables t,\n" +
                "    t_table_meta m\n" +
                "where\n" +
                "    c.table_id = t.id\n" +
                "        and m.ver_id = t.ver_id\n" +
                "        and m.column_name = c.field_name\n" +
                "        and c.table_id = ?";
        return query(sql, new Object[]{tableId}, rs -> {
            List<EncodeColumn> list = new ArrayList<>();
            while (rs.next()) {
                EncodeColumn col = new EncodeColumn();
                col.setId(rs.getLong("id"));
                col.setDesc(rs.getString("desc_"));
                col.setEncodeParam(rs.getString("encode_param"));
                col.setPluginId(rs.getLong("plugin_id"));
                col.setEncodeType(rs.getString("encode_type"));
                col.setTableId(rs.getLong("table_id"));
                col.setFieldName(rs.getString("field_name"));
                col.setUpdateTime(rs.getDate("update_time"));
                col.setLength(rs.getInt("data_length"));
                col.setTruncate(rs.getBoolean("truncate"));
                list.add(col);
            }
            return list;
        });
    }

    public void updateMetaChangeFlag(long tableId, int flag) {
        String sql = "update t_data_tables set meta_change_flg = ? where id = ?";
        executeUpdate(sql, flag, tableId);
    }

    public List<EncoderPluginConfig> loadEncodePlugins(Long projectId) {
        String sql = "select * from t_encode_plugins where project_id = ? and status= ?";
        return query(sql, new Object[]{projectId, EncoderPluginConfig.ACTIVE}, rs -> {
            List<EncoderPluginConfig> plugins = new LinkedList<>();
            while (rs.next()) {
                EncoderPluginConfig plugin = new EncoderPluginConfig();
                plugin.setId(rs.getLong("id"));
                plugin.setName(rs.getString("name"));
                plugin.setJarPath(rs.getString("path"));
                plugin.setProjectId(projectId);
                plugin.setStatus(rs.getString("status"));
                plugin.setUpdateTime(rs.getDate("update_time"));
                plugins.add(plugin);
            }
            return plugins;
        });
    }
}
