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

package com.creditease.dbus.stream.mysql.appender.meta;

import com.creditease.dbus.commons.MetaWrapper;
import com.creditease.dbus.stream.common.appender.bean.TableComments;
import com.creditease.dbus.stream.common.appender.meta.MetaFetcher;
import com.creditease.dbus.stream.common.appender.meta.MetaFetcherException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;

/**
 * Created by haowei6 on 2017/9/15.
 */
public class MysqlMetaFetcher implements MetaFetcher {

    private Logger logger = LoggerFactory.getLogger(MysqlMetaFetcher.class);
    private DataSource ds;

    public MysqlMetaFetcher(DataSource ds) throws Exception {
        this.ds = ds;
    }

    public DataSource getDataSource() {
        return ds;
    }

    @Override
    public MetaWrapper fetch2X(String schema, String tableName, int version) throws Exception {
        /**
         * 对于mysql源库，此方法不需要使用
         */
        return null;
    }

    @Override
    public MetaWrapper fetch(String schema, String table, int version) throws Exception {
        DataSource ds = getDataSource();
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            logger.debug(String.format("start to fetch Meta info[%s, %s]", schema, table));
            conn = ds.getConnection();
            String sql = "select t1.table_schema,\n" +
                    "       t1.table_name,\n" +
                    "       t1.column_name,\n" +
                    "       t1.ordinal_position,\n" +
                    "       t1.is_nullable,\n" +
                    "       t1.data_type,\n" +
                    "       t1.column_type,\n" +
                    "       t1.character_maximum_length,\n" +
                    "       t1.numeric_precision,\n" +
                    "       t1.numeric_scale,\n" +
                    ////mysql5.5没有这一列
                    //"       t1.datetime_precision,\n" +
                    "       t1.column_key,\n" +
                    "       t1.column_comment\n" +
                    "  from information_schema.columns t1\n" +
                    " where t1.table_schema = ?\n" +
                    "   and t1.table_name = ?\n";
            ps = conn.prepareStatement(sql);

            ps.setString(1, schema);
            ps.setString(2, table);

            ResultSet resultSet = ps.executeQuery();

            MetaWrapper meta = buildMeta(resultSet);
            if (meta == null) {
                throw new MetaFetcherException();
            }
            logger.debug(String.format("Meta[%s, %s]:%s", schema, table, meta.toString()));
//            meta.sortMetaCells();
            return meta;
        } catch (SQLException se) {
            logger.error("Fetch Mysql meta information error!", se);
            throw se;
        } finally {
            if (ps != null) ps.close();
            if (conn != null) conn.close();
        }
    }

    private MetaWrapper buildMeta(ResultSet rs) throws SQLException {
        MetaWrapper meta = new MetaWrapper();
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        while (rs.next()) {
            meta.addMetaCell(buildCell(rs, timestamp));
        }
        return meta;
    }

    private MetaWrapper.MetaCell buildCell(ResultSet rs, Timestamp timestamp) throws SQLException {
        MetaWrapper.MetaCell cell = new MetaWrapper.MetaCell();

        cell.setOwner(rs.getString("table_schema"));
        cell.setTableName(rs.getString("table_name"));
        cell.setColumnName(rs.getString("column_name"));
        cell.setOriginalColumnName(cell.getColumnName());

        Integer colId = rs.getInt("ordinal_position");
        cell.setColumnId(colId);
        cell.setInternalColumnId(colId);
        String columnType = rs.getString("column_type");
        String dataType = rs.getString("data_type");
        if("int".equalsIgnoreCase(dataType) && columnType.trim().toLowerCase().endsWith("unsigned")) {
            dataType = "int unsigned";
        }
        cell.setDataType(dataType);

        /**
         * 不同类型去源端查到的列不相同
         * 字符串：character_maximum_length
         * 整数，浮点数：numeric_precision numeric_scale
         * datatime，timestamp：datetime_precision
         */
        cell.setDataLength(0L);
        cell.setDataPrecision(0);
        cell.setDataScale(0);
        if(rs.getObject("character_maximum_length") != null) {
            cell.setDataLength(rs.getLong("character_maximum_length"));
        } else if(rs.getObject("numeric_precision") != null) {
            cell.setDataPrecision(rs.getInt("numeric_precision"));
            cell.setDataScale(rs.getInt("numeric_scale"));
        }
        //mysql5.5没有这一列
        //else if(rs.getObject("datetime_precision") != null) {
        //    cell.setDataPrecision(rs.getInt("datetime_precision"));
        //}

        if("yes".equalsIgnoreCase(rs.getString("is_nullable"))) {
            cell.setNullAble("Y");
        } else {
            cell.setNullAble("N");
        }

        if("pri".equalsIgnoreCase(rs.getString("column_key"))) {
            cell.setIsPk("Y");
        } else {
            cell.setIsPk("N");
        }

        cell.setDdlTime(timestamp);

        cell.setHiddenColumn("NO");
        cell.setVirtualColumn("NO");

        cell.setComments(rs.getString("column_comment"));
        return cell;
    }

    @Override
    public TableComments fetchTableComments(String schema, String table) throws Exception {
        DataSource ds = getDataSource();
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            logger.debug(String.format("start to fetch table comment [%s, %s]", schema, table));
            conn = ds.getConnection();
            String sql = "select TABLE_SCHEMA , TABLE_NAME, TABLE_COMMENT from information_schema.tables where TABLE_SCHEMA= ? and TABLE_NAME = ?";
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
            tableComments.setOwner(resultSet.getString("TABLE_SCHEMA"));
            tableComments.setTableName(resultSet.getString("TABLE_NAME"));
            tableComments.setComments(resultSet.getString("TABLE_COMMENT"));
            return tableComments;
        } catch (SQLException e) {
            logger.error("Fetch table comments error!", e);
            throw e;
        } finally {
            if (ps != null) ps.close();
            if (conn != null) conn.close();
        }
    }
}
