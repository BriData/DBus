package com.creditease.dbus.service.meta;

import com.creditease.dbus.commons.MetaWrapper;
import com.creditease.dbus.commons.SupportedOraDataType;
import com.creditease.dbus.domain.model.DataSource;
import com.creditease.dbus.domain.model.TableMeta;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This is Description
 *
 * @author xiancangao
 * @date 2018/12/19
 */
public class OracleMataFetcher extends MetaFetcher {
    public OracleMataFetcher(DataSource ds) {
        super(ds);
    }

    @Override
    public String buildQuery(Object... args) {
        String sql = "select t.owner,\n" +
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
                "  left join (select cu.owner, cu.table_name, cu.column_name, cu.position, 1 as is_pk from all_cons_columns cu, all_constraints au " +
                "  where cu.constraint_name = au.constraint_name and au.constraint_type = 'P' and cu.owner = au.owner " +
                "  and au.table_name = ? and au.owner = ? ) t1 on t.column_name = t1.column_name and t.table_name = t1.table_name and t.owner = t1.owner";
        return sql;
    }

    @Override
    public String fillParameters(PreparedStatement statement, Map<String, Object> params) throws Exception {
        statement.setString(1, get(params, "schemaName"));
        statement.setString(2, get(params, "tableName"));
        statement.setString(3, get(params, "tableName"));
        statement.setString(4, get(params, "schemaName"));
        return null;
    }

    @Override
    public List<TableMeta> buildResult(ResultSet rs) throws SQLException {
        List<TableMeta> list = new ArrayList<>();
        TableMeta meta;
        while (rs.next()) {
            meta = new TableMeta();

            meta.setOriginalColumnName(rs.getString("column_name"));
            meta.setColumnName(meta.getOriginalColumnName().replaceAll("[^A-Za-z0-9_]", "_"));

            Integer colId = rs.getInt("column_id");
            meta.setColumnId(colId == 0 ? 9999 : colId);

            //meta.setVersion(rs.getInt("version"));
            meta.setDataType(rs.getString("data_type"));
            meta.setDataLength(rs.getLong("data_length"));
            meta.setDataPrecision(rs.getInt("data_precision"));

            if (SupportedOraDataType.NUMBER.toString().equals(meta.getDataType())) {
                Object scale = rs.getObject("data_scale");
                meta.setDataScale(scale == null ? -127 : Integer.parseInt(scale.toString()));
            } else {
                meta.setDataScale(rs.getInt("data_scale"));
            }

            meta.setNullable(rs.getString("nullable"));
            //meta.setDdlTime(rs.getTimestamp("ddl_time"));
            meta.setIsPk(rs.getString("is_pk"));
            meta.setPkPosition(rs.getInt("pk_position"));
            meta.setCharLength(rs.getInt("char_length"));
            meta.setCharUsed(rs.getString("char_used"));
            meta.setInternalColumnId(rs.getInt("internal_column_id"));
            meta.setHiddenColumn(rs.getString("hidden_column"));
            meta.setVirtualColumn(rs.getString("virtual_column"));
            String defaultValue = rs.getString("data_default");
            if (defaultValue != null) {
                meta.setDefaultValue(defaultValue.trim());
            }

            // 2.X版本没有注释信息
            try {
                rs.findColumn("comments");
                meta.setComments(rs.getString("comments"));
            } catch (SQLException e) {
//            logger.warn("source is 2.X, comments does not exist", e);
            }
            list.add(meta);
        }
        return list;
    }

    private String get(Map<String, Object> map, String key) {
        return map.get(key).toString();
    }
}
