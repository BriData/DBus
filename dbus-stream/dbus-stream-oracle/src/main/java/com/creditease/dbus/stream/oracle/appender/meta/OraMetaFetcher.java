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

package com.creditease.dbus.stream.oracle.appender.meta;

import com.creditease.dbus.commons.MetaWrapper;
import com.creditease.dbus.commons.SupportedOraDataType;
import com.creditease.dbus.stream.common.appender.bean.TableComments;
import com.creditease.dbus.stream.common.appender.meta.MetaFetcher;
import com.creditease.dbus.stream.common.appender.meta.MetaFetcherException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by Shrimp on 16/5/17.
 */
public class OraMetaFetcher implements MetaFetcher {
    private Logger logger = LoggerFactory.getLogger(OraMetaFetcher.class);
    private DataSource ds;

    public OraMetaFetcher(DataSource ds) throws Exception {
        this.ds = ds;
    }

    public DataSource getDataSource() {
        return ds;
    }

    @Override
    public MetaWrapper fetch2X(String schema, String table, int version) throws Exception {
        DataSource ds = getDataSource();
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            logger.debug(String.format("start to fetch Meta info[%s, %s, %d]", schema, table, version));
            conn = ds.getConnection();
            String sql = "select t1.owner,\n" +
                    "       t1.table_name,\n" +
                    "       t1.column_name,\n" +
                    "       t1.column_id,\n" +
                    "       t1.internal_column_id,\n" +
                    "       t1.hidden_column,\n" +
                    "       t1.virtual_column,\n" +
                    "       t1.data_type,\n" +
                    "       t1.data_length,\n" +
                    "       t1.data_precision,\n" +
                    "       t1.data_scale,\n" +
                    "       t1.nullable,\n" +
                    "       t1.ddl_time,\n" +
                    "       t1.is_pk,\n" +
                    "       t1.pk_position,\n" +
                    "       t1.char_length,\n" +
                    "       t1.char_used,\n" +
                    "       '' data_default,\n" +
                    "       '0' is_current\n" +
                    "  from table_meta_his t1\n" +
                    " where t1.owner = ?\n" +
                    "   and t1.table_name = ?\n" +
                    "   and t1.version = ?\n" +
                    "union all\n" +
                    "select t1.owner,\n" +
                    "       t1.table_name,\n" +
                    "       t1.column_name,\n" +
                    "       t1.column_id,\n" +
                    "       t1.internal_column_id,\n" +
                    "       t1.hidden_column,\n" +
                    "       t1.virtual_column,\n" +
                    "       t1.data_type,\n" +
                    "       t1.data_length,\n" +
                    "       t1.data_precision,\n" +
                    "       t1.data_scale,\n" +
                    "       t1.nullable,\n" +
                    "       systimestamp,\n" +
                    "       decode(t2.is_pk, 1, 'Y', 'N') is_pk,\n" +
                    "       decode(t2.position, null, -1, t2.position) pk_position,\n" +
                    "       t1.char_length,\n" +
                    "       t1.char_used,\n" +
                    "       t1.data_default,\n" +
                    "       '1' is_current\n" +
                    "  from (select t.*\n" +
                    "          from all_tab_cols t\n" +
                    "         where t.owner = ?\n" +
                    "           and t.table_name =? ) t1\n" + // 需要查询出创建函数索引生成的虚拟列
                    "  left join (select cu.table_name, cu.column_name, cu.position, 1 as is_pk\n" +
                    "               from all_cons_columns cu, all_constraints au\n" +
                    "              where cu.constraint_name = au.constraint_name\n" +
                    "                and au.constraint_type = 'P'\n" +
                    "                and cu.owner = au.owner\n" +
                    "                and au.table_name = ?\n" +
                    "                and au.owner = ?) t2 on (t1.column_name = t2.column_name and t1.table_name = t2.table_name)\n";
            ps = conn.prepareStatement(sql);

            ps.setString(1, schema);
            ps.setString(2, table);
            ps.setInt(3, version);
            ps.setString(4, schema);
            ps.setString(5, table);
            ps.setString(6, table);
            ps.setString(7, schema);

            ResultSet resultSet = ps.executeQuery();

            MetaWrapper meta = buildMeta(resultSet);
            if (meta == null) {
                throw new MetaFetcherException();
            }
            logger.debug(String.format("Meta[%s, %s, %d]:%s", schema, table, version, meta.toString()));
            meta.sortMetaCells();
            return meta;
        } catch (SQLException se) {
            logger.error("Fetch Meta information error!", se);
            throw se;
        } finally {
            if (ps != null) ps.close();
            if (conn != null) conn.close();
        }
    }

    /**
     * 该方法用来获取源数据表的meta信息
     * 说明:
     * <p>
     * 代码中第一段sql语句查询符合版本的meta信息,第二段sql查询当前meta信息
     * 以下实例会说明为什么要合并这两段sql语句
     * eg: step1) 根据输入参数执行sql1查询table_meta_his无结果,此时说明我们要获取的版本
     * 在执行sql2之前源表添加了一个字段,导致sql2查询到的字段比实际version的版本多了一个字段
     * 其实这种情况,应该再次查询table_meta_his表获取指定version的信息,但是程序无法判断是否应该再次查询table_meta_his表,
     * 将两条sql合并后存在两种情况:
     * 1) 两条sql都查询到了结果,这种情况我们取结果集合中is_current == '0'的记录
     * 2) 第一条sql无法查询到结果,结果集中只存在is_current == 1的记录
     * </p>
     *
     * @param schema  schema名
     * @param table   表名
     * @param version osource抓取到的信息为上一版meta的版本号
     * @return 返回meta信息
     * @throws Exception
     */
    @Override
    public MetaWrapper fetch(String schema, String table, int version) throws Exception {
        DataSource ds = getDataSource();
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            logger.debug(String.format("start to fetch Meta info[%s, %s, %d]", schema, table, version));
            conn = ds.getConnection();
            String sql = "select t1.owner,\n" +
                    "       t1.table_name,\n" +
                    "       t1.column_name,\n" +
                    "       t1.column_id,\n" +
                    "       t1.internal_column_id,\n" +
                    "       t1.hidden_column,\n" +
                    "       t1.virtual_column,\n" +
                    "       t1.data_type,\n" +
                    "       t1.data_length,\n" +
                    "       t1.data_precision,\n" +
                    "       t1.data_scale,\n" +
                    "       t1.nullable,\n" +
                    "       t1.ddl_time,\n" +
                    "       t1.is_pk,\n" +
                    "       t1.pk_position,\n" +
                    "       t1.char_length,\n" +
                    "       t1.char_used,\n" +
                    "       '0' is_current,\n" +
                    "       null data_default,\n" +
                    "       t1.comments\n" +
                    "  from table_meta_his t1\n" +
                    " where t1.owner = ?\n" +
                    "   and t1.table_name = ?\n" +
                    "   and t1.version = ?\n" +
                    "union all\n" +
                    "select t.owner,\n" +
                    "       t.table_name,\n" +
                    "       t.column_name,\n" +
                    "       t.column_id,\n" +
                    "       t.internal_column_id,\n" +
                    "       t.hidden_column,\n" +
                    "       t.virtual_column,\n" +
                    "       t.data_type,\n" +
                    "       t.data_length,\n" +
                    "       t.data_precision,\n" +
                    "       t.data_scale,\n" +
                    "       t.nullable,\n" +
                    "       systimestamp,\n" +
                    "       decode(t1.is_pk, 1, 'Y', 'N') is_pk,\n" +
                    "       decode(t1.position, null, -1, t1.position) pk_position,\n" +
                    "       t.char_length,\n" +
                    "       t.char_used,\n" +
                    "       '1' is_current,\n" +
                    "       t.data_default,\n" +
                    "       tcc.comments\n" +
                    "  from (select * from all_tab_cols t where t.owner = ? and t.table_name = ? ) t \n" +
                    "  left join all_col_comments tcc on t.owner = tcc.owner and t.table_name = tcc.table_name and t.column_name = tcc.column_name \n" +
                    "  left join (select cu.owner, cu.table_name, cu.column_name, cu.position, 1 as is_pk from all_cons_columns cu, all_constraints au where cu.constraint_name = au.constraint_name and au.constraint_type = 'P' and cu.owner = au.owner and au.table_name = ? and au.owner = ? ) t1 on t.column_name = t1.column_name and t.table_name = t1.table_name and t.owner = t1.owner";
            ps = conn.prepareStatement(sql);

            ps.setString(1, schema);
            ps.setString(2, table);
            ps.setInt(3, version);
            ps.setString(4, schema);
            ps.setString(5, table);
            ps.setString(6, table);
            ps.setString(7, schema);

            ResultSet resultSet = ps.executeQuery();

            MetaWrapper meta = buildMeta(resultSet);
            if (meta == null) {
                throw new MetaFetcherException(String.format("no privilege on table: %s.%s", schema, table));
            }
            logger.debug(String.format("Meta[%s, %s, %d]:%s", schema, table, version, meta.toString()));
            meta.sortMetaCells();
            return meta;
        } catch (SQLException se) {
            logger.error("Fetch Meta information error!", se);
            throw se;
        } finally {
            if (ps != null) ps.close();
            if (conn != null) conn.close();
        }
    }


    private MetaWrapper buildMeta(ResultSet rs) throws SQLException {
        MetaWrapper currMeta = null; // 最新版本的meta
        MetaWrapper verMeta = null;  // 指定version版本的meta
        while (rs.next()) {
            int current = rs.getInt("is_current");
            if (current == 0) {
                if (verMeta == null) {
                    verMeta = new MetaWrapper();
                }

                verMeta.addMetaCell(buildCell(rs));
            } else {
                if (currMeta == null) {
                    currMeta = new MetaWrapper();
                }
                currMeta.addMetaCell(buildCell(rs));
            }
        }

        // 查询到指定版本的meta则返回,否则返回当前版本的meta信息
        if (verMeta != null) {
            // copy default value
            for (MetaWrapper.MetaCell cell : currMeta.getColumns()) {
                if (cell.getDefaultValue() != null && verMeta.contains(cell.getColumnName())) {
                    verMeta.get(cell.getColumnName()).setDefaultValue(cell.getDefaultValue());
                } else {
                    logger.warn("copy default value of {}", cell.getOwner() + "." + cell.getTableName() + "." + cell.getColumnName());
                }
            }
            return verMeta;
        } else {
            return currMeta;
        }
    }

    private MetaWrapper.MetaCell buildCell(ResultSet rs) throws SQLException {
        MetaWrapper.MetaCell cell = new MetaWrapper.MetaCell();

        cell.setOwner(rs.getString("owner"));
        cell.setTableName(rs.getString("table_name"));
        cell.setOriginalColumnName(rs.getString("column_name"));
        cell.setColumnName(cell.getOriginalColumnName().replaceAll("[^A-Za-z0-9_]", "_"));

        Integer colId = rs.getInt("column_id");
        cell.setColumnId(colId == 0 ? 9999 : colId);

        //cell.setVersion(rs.getInt("version"));
        cell.setDataType(rs.getString("data_type"));
        cell.setDataLength(rs.getLong("data_length"));
        cell.setDataPrecision(rs.getInt("data_precision"));

        if (SupportedOraDataType.NUMBER.toString().equals(cell.getDataType())) {
            Object scale = rs.getObject("data_scale");
            cell.setDataScale(scale == null ? -127 : Integer.parseInt(scale.toString()));
        } else {
            cell.setDataScale(rs.getInt("data_scale"));
        }

        cell.setNullAble(rs.getString("nullable"));
        cell.setDdlTime(rs.getTimestamp("ddl_time"));
        cell.setIsPk(rs.getString("is_pk"));
        cell.setPkPosition(rs.getInt("pk_position"));
        cell.setCharLength(rs.getInt("char_length"));
        cell.setCharUsed(rs.getString("char_used"));
        cell.setInternalColumnId(rs.getInt("internal_column_id"));
        cell.setHiddenColumn(rs.getString("hidden_column"));
        cell.setVirtualColumn(rs.getString("virtual_column"));
        String defaultValue = rs.getString("data_default");
        if (defaultValue != null) {
            cell.setDefaultValue(defaultValue.trim());
        }

        // 2.X版本没有注释信息
        try {
            rs.findColumn("comments");
            cell.setComments(rs.getString("comments"));
        } catch (SQLException e) {
//            logger.warn("source is 2.X, comments does not exist", e);
        }

        return cell;
    }

    /**
     * 从源端获取表的注释信息
     *
     * @param schema
     * @param table
     * @return
     * @throws Exception
     */
    @Override
    public TableComments fetchTableComments(String schema, String table) throws Exception {
        DataSource ds = getDataSource();
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            logger.debug(String.format("start to fetch table comment [%s, %s]", schema, table));
            conn = ds.getConnection();
            String sql = "select owner, table_name, comments from all_tab_comments where owner= ? and table_name = ?";
            ps = conn.prepareStatement(sql);
            ps.setString(1, schema);
            ps.setString(2, table);
            ResultSet resultSet = ps.executeQuery();

            if (!resultSet.next()) {
                TableComments tableComments = new TableComments();
                tableComments.setOwner(schema);
                tableComments.setTableName(table);
                return tableComments;
            }
            TableComments tableComments = new TableComments();
            tableComments.setOwner(resultSet.getString("owner"));
            tableComments.setTableName(resultSet.getString("table_name"));
            tableComments.setComments(resultSet.getString("comments"));
            return tableComments;
        } catch (SQLException se) {
            logger.error("Fetch table comments error!", se);
            throw se;
        } finally {
            if (ps != null) ps.close();
            if (conn != null) conn.close();
        }
    }
}
