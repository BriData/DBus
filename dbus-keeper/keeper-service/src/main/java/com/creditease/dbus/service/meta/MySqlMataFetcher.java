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
import java.sql.PreparedStatement;
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
        String sql ="select \n" +
                    "    t1.column_name,\n" +
                    "    t1.ordinal_position as column_id,\n" +
                    "    t1.is_nullable,\n" +
                    "    t1.data_type,\n" +
                    "    t1.character_maximum_length as data_length,\n" +
                    "    t1.numeric_precision as data_precision,\n" +
                    "    t1.numeric_scale as data_scale,\n" +
                    "    case\n" +
                    "        when t2.is_pk = 1 then 'Y'\n" +
                    "        else 'N'\n" +
                    "    end as is_pk,\n" +
                    "    t2.ORDINAL_POSITION as pk_position\n" +
                    "from\n" +
                    "    (select \n" +
                    "        *\n" +
                    "    from\n" +
                    "        information_schema.columns\n" +
                    "    where\n" +
                    "        table_schema = ?\n" +
                    "            and table_name = ?) as t1\n" +
                    "        left join\n" +
                    "    (select \n" +
                    "        t.table_schema,\n" +
                    "            t.table_name,\n" +
                    "            c.column_name,\n" +
                    "            1 as is_pk,\n" +
                    "            c.ORDINAL_POSITION\n" +
                    "    from\n" +
                    "        information_schema.table_constraints as t, information_schema.key_column_usage as c\n" +
                    "    where\n" +
                    "        t.table_name = c.table_name\n" +
                    "            and t.table_schema = c.table_schema\n" +
                    "            and t.table_schema = ?\n" +
                    "            and t.table_name = ?\n" +
                    "            and t.constraint_type = 'primary key') t2 ON (t1.table_schema = t2.table_schema\n" +
                    "        and t1.table_name = t2.table_name\n" +
                    "        and t1.column_name = t2.column_name)\n" +
                    "order by t1.ORDINAL_POSITION asc;";
        return sql;
    }

    @Override
    public String fillParameters(PreparedStatement statement, Map<String, Object> params) throws Exception {
        statement.setString(1, get(params, "schemaName"));
        statement.setString(2, get(params, "tableName"));
        statement.setString(3, get(params, "schemaName"));
        statement.setString(4, get(params, "tableName"));
        return null;
    }

    private String get(Map<String, Object> map, String key) {
        return map.get(key).toString();
    }
}
