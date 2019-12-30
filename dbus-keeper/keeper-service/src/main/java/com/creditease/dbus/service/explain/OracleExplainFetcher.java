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


package com.creditease.dbus.service.explain;

import com.creditease.dbus.domain.model.DataSource;
import com.creditease.dbus.domain.model.DataTable;

/**
 * This is Description
 *
 * @author xiancangao
 * @date 2018/12/28
 */
public class OracleExplainFetcher extends SqlExplainFetcher {
    public OracleExplainFetcher(DataSource ds) {
        super(ds);
    }

    @Override
    public String buildQuery(DataTable dataTable, String codition) {
        //String sql = "EXPLAIN PLAN FOR SELECT * FROM " +
        //        dataTable.getSchemaName() + "." + dataTable.getTableName() +
        //        " WHERE " + codition + " AND ROWNUM <=1 ";
        String sql = "SELECT * FROM " + dataTable.getSchemaName() + "." + dataTable.getTableName() +
                " WHERE ( " + codition + " ) AND 1 = 2 ";
        return sql;
    }

}
