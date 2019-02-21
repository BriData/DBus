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
        String sql = "SELECT * FROM " + dataTable.getSchemaName() + "." + dataTable.getTableName() +
                " WHERE 1 = 2 AND ( " + codition + " )";
        return sql;
    }

}
