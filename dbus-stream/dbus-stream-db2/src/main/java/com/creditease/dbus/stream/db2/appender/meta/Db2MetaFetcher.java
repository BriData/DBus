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


package com.creditease.dbus.stream.db2.appender.meta;

import com.creditease.dbus.commons.MetaWrapper;
import com.creditease.dbus.stream.common.appender.bean.TableComments;
import com.creditease.dbus.stream.common.appender.meta.MetaFetcher;
import com.creditease.dbus.stream.common.appender.meta.MetaFetcherException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;

/**
 * Created by zhenlinzhong on 2018/6/14.
 */

public class
Db2MetaFetcher implements MetaFetcher {

    private Logger logger = LoggerFactory.getLogger(Db2MetaFetcher.class);
    private DataSource ds;

    public Db2MetaFetcher(DataSource ds) throws Exception {
        this.ds = ds;
    }

    public DataSource getDataSource() {
        return ds;
    }

    @Override
    public MetaWrapper fetch2X(String schema, String tableName, int version) throws Exception {
        /**
         * db2不需要使用
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
            //pk的位置不要动，db2根据别名获取字段信息有问题，这里根据序号获取数据
            String sql = "select A.table_schema,\n" +
                    "      A.table_name,\n" +
                    "      A.column_name,\n" +
                    "      A.ordinal_position,\n" +
                    "      A.is_nullable,\n" +
                    "      A.data_type,\n" +
                    "      A.character_maximum_length,\n" +
                    "      A.numeric_precision,\n" +
                    "      A.numeric_scale,\n" +
                    "      pk,\n" +
                    "      A.datetime_precision\n" +
                    "FROM SYSIBM.columns A LEFT JOIN " +
                    "(SELECT B.TABSCHEMA, B.TABNAME, COLNAME AS pk FROM SYSCAT.tabconst B, SYSCAT.keycoluse C WHERE B.CONSTNAME = C.CONSTNAME AND B.TYPE='P' ) t " +
                    "ON t.TABSCHEMA=A.TABLE_SCHEMA AND t.TABNAME=A.TABLE_NAME AND t.PK=A.COLUMN_NAME WHERE A.TABLE_SCHEMA=? AND A.TABLE_NAME=?\n";
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
            logger.error("Fetch db2 meta information error!", se);
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
        cell.setDataType(rs.getString("data_type"));

        /**
         * 不同类型去源端查到的列不相同
         * 字符串：character_maximum_length
         * 整数，浮点数：numeric_precision numeric_scale
         * datatime，timestamp：datetime_precision
         */
        cell.setDataLength(0L);
        cell.setDataPrecision(0);
        cell.setDataScale(0);
        if (rs.getObject("character_maximum_length") != null) {
            cell.setDataLength(rs.getLong("character_maximum_length"));
        } else if (rs.getObject("numeric_precision") != null) {
            cell.setDataPrecision(rs.getInt("numeric_precision"));
            cell.setDataScale(rs.getInt("numeric_scale"));
        } else if (rs.getObject("datetime_precision") != null) {
            cell.setDataPrecision(rs.getInt("datetime_precision"));
        }

        if ("yes".equalsIgnoreCase(rs.getString("is_nullable"))) {
            cell.setNullAble("Y");
        } else {
            cell.setNullAble("N");
        }

        //db2，根据序号获取主键信息，根据别名获取主键信息的方式不可用
        if (rs.getString(10) == null) {
            cell.setIsPk("N");
        } else {
            cell.setIsPk("Y");
        }

        cell.setDdlTime(timestamp);

        cell.setHiddenColumn("NO");
        cell.setVirtualColumn("NO");
        //db2中没有column_comment字段,为避免其他地方引用，这里直接设为空
        cell.setComments("");
        return cell;
    }

    @Override
    public TableComments fetchTableComments(String schema, String table) throws Exception {
        /**
         * db2中暂时用不到column_comment字段
         */
        return null;
    }
}
