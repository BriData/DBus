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

package com.creditease.dbus.service.meta;

import com.creditease.dbus.domain.model.DataSource;
import com.creditease.dbus.domain.model.TableMeta;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by zhangyf on 16/9/19.
 */
public class MySqlMataFetcher extends MetaFetcher {
    public MySqlMataFetcher(DataSource ds) {
        super(ds);
    }

    @Override
    public String buildQuery(Object... args) {
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
                "       t1.column_key,\n" +
                "       t1.column_comment\n" +
                "  from information_schema.columns t1\n" +
                " where t1.table_schema = ?\n" +
                "   and t1.table_name = ?\n";
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
            meta.setOriginalColumnName(rs.getString("column_name"));
            meta.setColumnId(rs.getInt("ordinal_position"));
            meta.setInternalColumnId(rs.getInt("ordinal_position"));
            meta.setHiddenColumn("NO");
            meta.setVirtualColumn("NO");
            //meta.setOriginalSer(0L);
            String columnType = rs.getString("column_type");
            String dataType = rs.getString("data_type");
            if ("int".equalsIgnoreCase(dataType) && columnType.trim().toLowerCase().endsWith("unsigned")) {
                dataType = "int unsigned";
            }
            meta.setDataType(dataType);
            /**
             * 不同类型去源端查到的列不相同
             * 字符串：character_maximum_length
             * 整数，浮点数：numeric_precision numeric_scale
             * datatime，timestamp：datetime_precision
             */
            meta.setDataLength(0L);
            meta.setDataPrecision(0);
            meta.setDataScale(0);
            if (rs.getObject("character_maximum_length") != null) {
                meta.setDataLength(rs.getLong("character_maximum_length"));
            } else if (rs.getObject("numeric_precision") != null) {
                meta.setDataPrecision(rs.getInt("numeric_precision"));
                meta.setDataScale(rs.getInt("numeric_scale"));
            }
            //mysql5.5版本没有这一列
            //else if (rs.getObject("datetime_precision") != null) {
            //    meta.setDataPrecision(rs.getInt("datetime_precision"));
            //}
            if("yes".equalsIgnoreCase(rs.getString("is_nullable"))) {
                meta.setNullable("Y");
            } else {
                meta.setNullable("N");
            }
            if("pri".equalsIgnoreCase(rs.getString("column_key"))) {
                meta.setIsPk("Y");
            } else {
                meta.setIsPk("N");
            }
            meta.setAlterTime(new Date());
            meta.setComments(rs.getString("column_comment"));
            list.add(meta);
        }
        return list;
    }

    private String get(Map<String, Object> map, String key) {
        return map.get(key).toString();
    }

}
