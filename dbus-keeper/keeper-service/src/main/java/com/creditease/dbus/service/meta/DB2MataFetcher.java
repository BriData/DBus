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


package com.creditease.dbus.service.meta;

import com.creditease.dbus.domain.model.DataSource;
import com.creditease.dbus.domain.model.TableMeta;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by zhangyf on 16/9/19.
 */
public class DB2MataFetcher extends MetaFetcher {
    public DB2MataFetcher(DataSource ds) {
        super(ds);
    }

    @Override
    public String buildQuery(Object... args) {
        //pk的位置不要动,db2根据别名获取字段信息有问题,这里根据序号获取数据
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
                "ON t.TABSCHEMA=A.TABLE_SCHEMA AND t.TABNAME=A.TABLE_NAME AND t.PK=A.COLUMN_NAME WHERE A.TABLE_SCHEMA=? AND A.TABLE_NAME=?";
        return sql;
    }

    @Override
    public String fillParameters(PreparedStatement statement, Map<String, Object> params) throws Exception {
        statement.setString(1, get(params, "schemaName"));
        statement.setString(2, get(params, "tableName"));
        return null;
    }

    @Override
    public List<TableMeta> buildResult(ResultSet rs) throws SQLException {
        List<TableMeta> list = new ArrayList<>();
        TableMeta meta;
        while (rs.next()) {
            meta = new TableMeta();
            meta.setColumnName(rs.getString("column_name"));
            meta.setOriginalColumnName(meta.getColumnName());

            Integer colId = rs.getInt("ordinal_position");
            meta.setColumnId(colId);
            meta.setInternalColumnId(colId);
            meta.setDataType(rs.getString("data_type"));

            /**
             * 不同类型去源端查到的列不相同
             * 字符串：character_maximum_length
             * 整数,浮点数：numeric_precision numeric_scale
             * datatime,timestamp：datetime_precision
             */
            meta.setDataLength(0L);
            meta.setDataPrecision(0);
            meta.setDataScale(0);
            if (rs.getObject("character_maximum_length") != null) {
                meta.setDataLength(rs.getLong("character_maximum_length"));
            } else if (rs.getObject("numeric_precision") != null) {
                meta.setDataPrecision(rs.getInt("numeric_precision"));
                meta.setDataScale(rs.getInt("numeric_scale"));
            } else if (rs.getObject("datetime_precision") != null) {
                meta.setDataPrecision(rs.getInt("datetime_precision"));
            }

            if ("yes".equalsIgnoreCase(rs.getString("is_nullable"))) {
                meta.setNullable("Y");
            } else {
                meta.setNullable("N");
            }

            //db2,根据序号获取主键信息,根据别名获取主键信息的方式不可用
            if (rs.getString(10) == null) {
                meta.setIsPk("N");
            } else {
                meta.setIsPk("Y");
            }

            //meta.setDdlTime(timestamp);

            meta.setHiddenColumn("NO");
            meta.setVirtualColumn("NO");
            //db2中没有column_comment字段,为避免其他地方引用,这里直接设为空
            meta.setComments("");
            list.add(meta);
        }
        return list;
    }

    private String get(Map<String, Object> map, String key) {
        return map.get(key).toString();
    }
}
