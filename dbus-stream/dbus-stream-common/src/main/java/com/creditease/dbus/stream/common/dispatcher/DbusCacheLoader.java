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


package com.creditease.dbus.stream.common.dispatcher;

import com.creditease.dbus.stream.common.appender.bean.DataTable;
import com.creditease.dbus.stream.common.appender.exception.RuntimeSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class DbusCacheLoader {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private DataSource ds;

    private interface ResultSetConverter<T> {
        T convert(ResultSet rs) throws Exception;
    }

    public DbusCacheLoader(DataSource dataSource) {
        this.ds = dataSource;
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

    /**
     * 查询数据库获取全部DataTable对象
     */
    public List<DataTable> queryDataTable(long dsId) {
        String sql = "select * from t_data_tables t where t.ds_id = ?";
        return query(sql, new Object[]{dsId}, rs -> {
            List<DataTable> list = new ArrayList<>();
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
                t.setIsOpen(rs.getInt("is_open"));
                t.setIsAutocomplete(rs.getBoolean("is_auto_complete"));
                if (ts != null) {
                    t.setCreateTime(new Date(ts.getTime()));
                }
                list.add(t);
            }
            return list;
        });
    }

}
