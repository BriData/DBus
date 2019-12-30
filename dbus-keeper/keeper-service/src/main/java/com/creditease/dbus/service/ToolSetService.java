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


package com.creditease.dbus.service;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.domain.mapper.DataTableMapper;
import com.creditease.dbus.domain.model.DataSource;
import com.creditease.dbus.domain.model.DataTable;
import com.creditease.dbus.domain.model.TableMeta;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.service.meta.MetaFetcher;
import com.creditease.dbus.utils.ConfUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Created by xiancangao on 2018/05/04.
 */
@Service
public class ToolSetService {
    @Autowired
    private DataTableMapper dataTableMapper;

    @Autowired
    private Environment environment;

    @Autowired
    private IZkService zkService;

    private Logger logger = LoggerFactory.getLogger(getClass());

    public HashMap<String, String> getAllUniqueColumn() {
        List<DataTable> allTables = dataTableMapper.findAllTables();
        allTables = allTables.stream().filter(dataTable -> dataTable.getDsType().equalsIgnoreCase("mysql") || dataTable.getDsType().equalsIgnoreCase("oracle")).collect(Collectors.toList());
        HashMap<String, String> map = new HashMap<>();
        allTables.forEach(dataTable -> {
            try {
                String uniqueColumn = getUniqueColumn(dataTable);
                map.put(dataTable.getId() + "-" + dataTable.getTableName(), uniqueColumn);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        return map;

    }

    public String getUniqueColumn(DataTable dataTable) throws Exception {
        String column = "";
        String tableName = dataTable.getTableName();
        // 对于系列表,任取其中一个表来获取Meta信息。此处取第一个表。
        if (tableName.indexOf(Constants.TABLE_SPLITTED_PHYSICAL_TABLES_SPLITTER) != -1) {
            tableName = tableName.split(Constants.TABLE_SPLITTED_PHYSICAL_TABLES_SPLITTER)[0];
        }
        Connection connection = null;
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        try {
            if (dataTable.getDsType().equalsIgnoreCase("mysql")) {
                connection = createDBConnection(dataTable);
            } else if (dataTable.getDsType().equalsIgnoreCase("oracle")) {
                Class.forName("oracle.jdbc.driver.OracleDriver");
                connection = createDBConnection(dataTable);
            } else if (dataTable.getDsType().equalsIgnoreCase("db2")) {
                Class.forName("com.ibm.db2.jcc.DB2Driver");
                connection = createDBConnection(dataTable);
            }
            if (tableName != null) {
                column = queryUniqueColumn(connection, dataTable, tableName, KeeperConstants.SPLIT_COL_TYPE_PK);
                if (StringUtils.isNotBlank(column)) {
                    return column;
                }
                column = queryUniqueColumn(connection, dataTable, tableName.toUpperCase(), KeeperConstants.SPLIT_COL_TYPE_PK);
                if (StringUtils.isNotBlank(column)) {
                    return column;
                }
                column = queryUniqueColumn(connection, dataTable, tableName, KeeperConstants.SPLIT_COL_TYPE_UK);
                if (StringUtils.isNotBlank(column)) {
                    return column;
                }
                column = queryUniqueColumn(connection, dataTable, tableName.toUpperCase(), KeeperConstants.SPLIT_COL_TYPE_UK);
                if (StringUtils.isNotBlank(column)) {
                    return column;
                }
            }
            return null;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return "";
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    public HashMap<String, Object> sourceTableColumn(Integer tableId, Integer number) throws Exception {
        DataTable dataTable = dataTableMapper.findById(tableId);
        DataSource dataSource = new DataSource();
        dataSource.setMasterUrl(dataTable.getMasterUrl());
        dataSource.setSlaveUrl(dataTable.getSlaveUrl());
        dataSource.setDbusUser(dataTable.getDbusUser());
        dataSource.setDbusPwd(dataTable.getDbusPassword());
        dataSource.setDsType(dataTable.getDsType());
        MetaFetcher fetcher = MetaFetcher.getFetcher(dataSource);
        HashMap<String, Object> params = new HashMap<>();
        params.put("schemaName", dataTable.getSchemaName());
        params.put("tableName", dataTable.getTableName());
        List<TableMeta> tableMetaList = fetcher.fetchMeta(params);
        Map<String, String> columTypes = tableMetaList.stream().collect(Collectors.toMap(tableMeta -> tableMeta.getColumnName(), tableMeta -> tableMeta.getDataType()));
        //获取分片列
        String splitCol = "";
        String tableName = dataTable.getTableName();
        // 对于系列表,任取其中一个表来获取Meta信息。此处取第一个表。
        if (tableName.indexOf(Constants.TABLE_SPLITTED_PHYSICAL_TABLES_SPLITTER) != -1) {
            tableName = tableName.split(Constants.TABLE_SPLITTED_PHYSICAL_TABLES_SPLITTER)[0];
        }
        Connection connection = null;
        HashMap<String, Object> result = new HashMap<>();
        HashMap<String, String> map = new HashMap<>();
        try {
            if (dataTable.getDsType().equalsIgnoreCase("mysql")) {
                connection = createDBConnection(dataTable);
            } else if (dataTable.getDsType().equalsIgnoreCase("oracle")) {
                Class.forName("oracle.jdbc.driver.OracleDriver");
                connection = createDBConnection(dataTable);
            } else if (dataTable.getDsType().equalsIgnoreCase("db2")) {
                Class.forName("com.ibm.db2.jcc.DB2Driver");
                connection = createDBConnection(dataTable);
            }
            if (tableName != null) {
                queryIndexedColForSplit(connection, dataTable, tableName, KeeperConstants.SPLIT_COL_TYPE_PK, map);
                queryIndexedColForSplit(connection, dataTable, tableName.toUpperCase(), KeeperConstants.SPLIT_COL_TYPE_PK, map);
                queryIndexedColForSplit(connection, dataTable, tableName, KeeperConstants.SPLIT_COL_TYPE_UK, map);
                queryIndexedColForSplit(connection, dataTable, tableName.toUpperCase(), KeeperConstants.SPLIT_COL_TYPE_UK, map);
                queryIndexedColForSplit(connection, dataTable, tableName, KeeperConstants.SPLIT_COL_TYPE_COMMON_INDEX, map);
                queryIndexedColForSplit(connection, dataTable, tableName.toUpperCase(), KeeperConstants.SPLIT_COL_TYPE_COMMON_INDEX, map);
            }
            if (map.size() == 0) {
                return result;
            }
            splitCol = getSplitColumn(connection, map, dataTable);
            ArrayList<LinkedHashMap> columnList = this.querySourceDb(dataTable, map, columTypes, number);
            result.put("splitColumn", splitCol);
            result.put("columns", columnList);
            return result;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return result;
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    public ArrayList<LinkedHashMap> querySourceDb(DataTable dataTable, Map<String, String> map, Map<String, String> columTypes, Integer number) {
        Connection connection = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String sql = null;
        ArrayList<LinkedHashMap> columnList = new ArrayList<>();
        try {
            StringBuilder sb = new StringBuilder();
            for (String col : map.keySet()) {
                sb.append(col).append(",");
            }
            String cols = sb.substring(0, sb.length() - 1).toString();
            if (dataTable.getDsType().equalsIgnoreCase("mysql")) {
                connection = createDBConnection(dataTable);
                sql = "select " + cols + " from " + dataTable.getSchemaName() + "." + dataTable.getTableName() + " limit ?";
            } else if (dataTable.getDsType().equalsIgnoreCase("oracle")) {
                Class.forName("oracle.jdbc.driver.OracleDriver");
                connection = createDBConnection(dataTable);
                sql = "select " + cols + " from " + dataTable.getSchemaName() + "." + dataTable.getTableName() + " where rownum < ?";
            } else if (dataTable.getDsType().equalsIgnoreCase("db2")) {
                Class.forName("com.ibm.db2.jcc.DB2Driver");
                connection = createDBConnection(dataTable);
                sql = "select " + cols + " from " + dataTable.getSchemaName() + "." + dataTable.getTableName() + " LIMIT ?";
            }
            logger.info("sql:{}", sql);
            stmt = connection.prepareStatement(sql);

            stmt.setFetchSize(2500);
            logger.info("Using fetchSize for next query: {}", 2500);
            stmt.setQueryTimeout(3600);
            logger.info("Using queryTimeout 3600 seconds");

            stmt.setInt(1, number);
            rs = stmt.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            while (rs.next()) {
                LinkedHashMap<String, Object> columns = new LinkedHashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = rsmd.getColumnName(i);
                    Object obj = rs.getObject(i);
                    String columnType = map.get(columnName);
                    String type = null;
                    if (columnType.contains(KeeperConstants.SPLIT_COL_TYPE_PK)) {
                        if (type != null) {
                            type = type + "," + "PK";
                        } else {
                            type = "PK";
                        }
                    }
                    if (columnType.contains(KeeperConstants.SPLIT_COL_TYPE_UK)) {
                        if (type != null) {
                            type = type + "," + "UK";
                        } else {
                            type = "UK";
                        }
                    }
                    if (columnType.contains(KeeperConstants.SPLIT_COL_TYPE_COMMON_INDEX)) {
                        if (type != null) {
                            type = type + "," + "COMMON_INDEX";
                        } else {
                            type = "COMMON_INDEX";
                        }
                    }
                    columns.put(columnName + "(" + columTypes.get(columnName) + "," + type + ")", obj != null ? obj.toString() : null);
                }
                columnList.add(columns);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            try {
                if (rs != null) rs.close();
                if (stmt != null) stmt.close();
                if (connection != null) connection.close();
            } catch (Exception e) {
                logger.info("Close con/statement encountered exception.", e);
            }
            logger.info("Query finished.");
            return columnList;
        }
    }

    private Connection createDBConnection(DataTable dataTable) throws Exception {
        try {
            return DriverManager.getConnection(dataTable.getSlaveUrl(), dataTable.getDbusUser(), dataTable.getDbusPassword());
        } catch (SQLException e) {
            logger.error("Connection to Oracle DB failed!", e);
            throw e;
        }
    }

    private String getSplitColumn(Connection conn, HashMap<String, String> result, DataTable dataTable) {

        ResultSet rsetOracle = null;
        PreparedStatement pStmtOracle = null;
        try {
            if (result.size() == 0) {
                logger.warn("Table has no  key, type ");
                return null;
            }
            if (dataTable.getDsType().equalsIgnoreCase("mysql")
                    || dataTable.getDsType().equalsIgnoreCase("db2")
            ) {
                int index = 0;
                Set<Map.Entry<String, String>> entries = result.entrySet();
                for (Map.Entry<String, String> entry : entries) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    logger.info("find split column :" + key + ", Index type:" + value);
                    if (++index == 1) {
                        return key;
                    }
                }
            }
            String splitCol = null;
            Set<Map.Entry<String, String>> entries = result.entrySet();
            for (Map.Entry<String, String> entry : entries) {
                String key = entry.getKey();
                String value = entry.getValue();

                // 对于ORACLE数据库,目前只有整数类型比较友好。区别对待下整数类型分片列和其他类型分片列
                String splitColTypeDetectQuery = "select " + key + " from " + dataTable.getSchemaName() + "." + dataTable.getTableName() + " where rownum <= 1";
                pStmtOracle = conn.prepareStatement(splitColTypeDetectQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                rsetOracle = pStmtOracle.executeQuery();
                while (rsetOracle.next()) {
                    int splitColSqlDataType = rsetOracle.getMetaData().getColumnType(1);
                    if (splitColSqlDataType == Types.INTEGER
                            || splitColSqlDataType == Types.TINYINT
                            || splitColSqlDataType == Types.SMALLINT
                            || splitColSqlDataType == Types.BIGINT
                            || splitColSqlDataType == Types.NUMERIC
                            || splitColSqlDataType == Types.DECIMAL
                            || splitColSqlDataType == Types.REAL
                            || splitColSqlDataType == Types.FLOAT
                            || splitColSqlDataType == Types.DOUBLE) {
                        // 对于上述数字类型,DBUS根据 splitCol 按分片策略分片并发拉取。
                        // 此处故意留白
                        logger.info("Found split column data type is {}(Numeric):", splitColSqlDataType);
                        logger.info("find split column :" + key + ", Index type:" + value);
                        return key;
                    } else {
                        // 对于整数以外的其它类型,将splitCol设为null。后续逻辑认为没有合适的分片列。将不对数据进行分片,所有数据作一片拉取。
                        logger.info("Found split column data type is {}(None Numeric):", splitColSqlDataType);
                    }
                }
            }
            return splitCol;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null;
        } finally {
            try {
                if (rsetOracle != null) {
                    rsetOracle.close();
                }
                if (pStmtOracle != null) {
                    pStmtOracle.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private String queryUniqueColumn(Connection conn, DataTable table, String tableName, String indexType) {

        PreparedStatement pStmt = null;
        ResultSet rset = null;


        String schema = table.getSchemaName();
        String shortTableName = tableName;
        int qualifierIndex = tableName.indexOf('.');
        if (qualifierIndex != -1) {
            schema = tableName.substring(0, qualifierIndex);
            shortTableName = tableName.substring(qualifierIndex + 1);
        }
        try {
            String indexedColQuery = null;
            DbusDatasourceType dataBaseType = DbusDatasourceType.valueOf(table.getDsType().toUpperCase());
            if (dataBaseType == DbusDatasourceType.MYSQL) {
                indexedColQuery = getMysqlIndexedColQuery(indexType);
            } else if (dataBaseType == DbusDatasourceType.ORACLE) {
                indexedColQuery = getOracleIndexedColQuery(indexType);
            } else if (dataBaseType == DbusDatasourceType.DB2) {
                indexedColQuery = getDB2IndexedColQuery(indexType);
            }
            pStmt = conn.prepareStatement(indexedColQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            pStmt.setString(1, shortTableName);
            pStmt.setString(2, schema);
            rset = pStmt.executeQuery();

            StringBuilder sb = new StringBuilder();
            String columnName = null;
            while (rset.next()) {
                if (dataBaseType == DbusDatasourceType.DB2) {
                    /**
                     * db2查询出来的列名是加号开头的,并且多个列名在一个结果行中,所以需要特殊处理
                     */
                    String[] stringArray = rset.getString(1).split("\\+");
                    for (String string : stringArray) {
                        if (StringUtils.isNotEmpty(string)) {
                            columnName = string;
                        }
                    }
                }
                if (dataBaseType == DbusDatasourceType.ORACLE || dataBaseType == DbusDatasourceType.MYSQL) {
                    columnName = rset.getString(1);
                }
                sb.append(columnName).append(",");
            }
            if (sb.length() > 0) {
                return sb.substring(0, sb.length() - 1);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            try {
                if (rset != null) {
                    rset.close();
                }
                if (pStmt != null) {
                    pStmt.close();
                }
            } catch (SQLException ex) {
                logger.error("Failed to close statement:{}", ex);
            }
        }
        return null;
    }


    private void queryIndexedColForSplit(Connection conn, DataTable table, String tableName, String indexType, Map<String, String> result) {

        PreparedStatement pStmt = null;
        ResultSet rset = null;

        String schema = table.getSchemaName();
        String shortTableName = tableName;
        int qualifierIndex = tableName.indexOf('.');
        if (qualifierIndex != -1) {
            schema = tableName.substring(0, qualifierIndex);
            shortTableName = tableName.substring(qualifierIndex + 1);
        }
        try {
            String indexedColQuery = null;
            DbusDatasourceType dataBaseType = DbusDatasourceType.valueOf(table.getDsType().toUpperCase());
            if (dataBaseType == DbusDatasourceType.MYSQL) {
                indexedColQuery = getMysqlIndexedColQuery(indexType);
            } else if (dataBaseType == DbusDatasourceType.ORACLE) {
                indexedColQuery = getOracleIndexedColQuery(indexType);
            } else if (dataBaseType == DbusDatasourceType.DB2) {
                indexedColQuery = getDB2IndexedColQuery(indexType);
            }
            pStmt = conn.prepareStatement(indexedColQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            pStmt.setString(1, shortTableName);
            pStmt.setString(2, schema);
            rset = pStmt.executeQuery();

            while (rset.next()) {
                if (dataBaseType == DbusDatasourceType.DB2) {
                    /**
                     * db2查询出来的列名是加号开头的,并且多个列名在一个结果行中,所以需要特殊处理
                     */
                    String[] stringArray = rset.getString(1).split("\\+");
                    for (String string : stringArray) {
                        if (StringUtils.isNotEmpty(string)) {
                            if (result.containsKey(string)) {
                                result.put(string, result.get(string) + "," + indexType);
                            } else {
                                result.put(string, indexType);
                            }
                        }
                    }
                }
                if (dataBaseType == DbusDatasourceType.ORACLE || dataBaseType == DbusDatasourceType.MYSQL) {
                    String columnName = rset.getString(1);
                    if (result.containsKey(columnName)) {
                        result.put(columnName, result.get(columnName) + "," + indexType);
                    } else {
                        result.put(columnName, indexType);
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            try {
                if (rset != null) {
                    rset.close();
                }
                if (pStmt != null) {
                    pStmt.close();
                }
            } catch (SQLException ex) {
                logger.error("Failed to close statement:{}", ex);
            }
        }
    }

    public String getMysqlIndexedColQuery(String indexType) {
        if (KeeperConstants.SPLIT_COL_TYPE_PK.equals(indexType)) {
            return MySQLManager.QUERY_PRIMARY_KEY_FOR_TABLE;
        }
        if (KeeperConstants.SPLIT_COL_TYPE_UK.equals(indexType)) {
            return MySQLManager.QUERY_UNIQUE_KEY_FOR_TABLE;
        }
        if (KeeperConstants.SPLIT_COL_TYPE_COMMON_INDEX.equals(indexType)) {
            return MySQLManager.QUERY_INDEXED_COL_FOR_TABLE;
        }
        return null;
    }

    public String getOracleIndexedColQuery(String indexType) {
        if (KeeperConstants.SPLIT_COL_TYPE_PK.equals(indexType)) {
            return OracleManager.QUERY_PRIMARY_KEY_FOR_TABLE;
        }
        if (KeeperConstants.SPLIT_COL_TYPE_UK.equals(indexType)) {
            return OracleManager.QUERY_UNIQUE_KEY_FOR_TABLE;
        }
        if (KeeperConstants.SPLIT_COL_TYPE_COMMON_INDEX.equals(indexType)) {
            return OracleManager.QUERY_INDEXED_COL_FOR_TABLE;
        }
        return null;
    }

    public String getDB2IndexedColQuery(String indexType) {
        if (KeeperConstants.SPLIT_COL_TYPE_PK.equals(indexType)) {
            return DB2Manager.QUERY_PRIMARY_KEY_FOR_TABLE;
        }
        if (KeeperConstants.SPLIT_COL_TYPE_UK.equals(indexType)) {
            return DB2Manager.QUERY_UNIQUE_KEY_FOR_TABLE;
        }
        if (KeeperConstants.SPLIT_COL_TYPE_COMMON_INDEX.equals(indexType)) {
            return DB2Manager.QUERY_INDEXED_COL_FOR_TABLE;
        }
        return null;
    }

    public HashMap<String, String> getMgrDBMsg() throws Exception {
        HashMap<String, String> map = new HashMap<>();
        map.put("spring.datasource.url", environment.getProperty("spring.datasource.url"));
        map.put("spring.datasource.username", environment.getProperty("spring.datasource.username"));
        map.put("spring.datasource.password", environment.getProperty("spring.datasource.password"));
        map.put("spring.datasource.driver-class-name", environment.getProperty("spring.datasource.driver-class-name"));
        map.put("zk.str", environment.getProperty("zk.str"));
        if (zkService.isExists("/DBusInit")) {
            for (Map.Entry<Object, Object> entry : zkService.getProperties("/DBusInit").entrySet()) {
                map.put((String) entry.getKey(), StringUtils.replace((String) entry.getValue(), "\\", ""));
            }
        } else if (zkService.isExists("/DBus")) {
            for (Map.Entry<Object, Object> entry : zkService.getProperties(Constants.GLOBAL_PROPERTIES_ROOT).entrySet()) {
                map.put((String) entry.getKey(), StringUtils.replace((String) entry.getValue(), "\\", ""));
            }
        }
        return map;
    }

    public void initMgrSql() throws Exception {
        Connection conn = null;
        String url = environment.getProperty("spring.datasource.url");
        String username = environment.getProperty("spring.datasource.username");
        String password = environment.getProperty("spring.datasource.password");
        try {
            conn = DriverManager.getConnection(url, username, password);
            List<String> sqls = readSqlsFromFile(ConfUtils.getParentPath() + "/conf/init/dbus_mgr.sql");
            batchDate(conn, sqls);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    public void batchDate(Connection connection, List<String> sql) throws Exception {
        try {
            Statement st = connection.createStatement();
            for (String subsql : sql) {
                st.addBatch(subsql);
            }
            st.executeBatch();
        } catch (SQLException e) {
            throw e;
        }
    }

    public List<String> readSqlsFromFile(String filePath) throws Exception {
        List<String> sqls = new ArrayList<>();
        StringBuffer sb = new StringBuffer();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), "UTF-8"));
            String tempString = null;
            while ((tempString = reader.readLine()) != null) {
                if (tempString.trim().equals("") || tempString.trim().startsWith("--")) {
                    continue;
                }
                if (tempString.endsWith(";")) {
                    sb.append(tempString);
                    sqls.add(sb.toString());
                    sb.delete(0, sb.length());
                } else {
                    sb.append(tempString);
                }
            }
        } catch (IOException e) {
            throw e;
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    logger.error(e1.getMessage(), e1);
                }
            }
        }
        return sqls;
    }

    public static class MySQLManager {
        /**
         * Query to find the primary key column name for a given table. This query
         * is restricted to the current schema.
         */
        public static final String QUERY_PRIMARY_KEY_FOR_TABLE =
                "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS "
                        + " WHERE TABLE_NAME = ? "
                        + " AND TABLE_SCHEMA = ? "
                        + " AND COLUMN_KEY = 'PRI'";

        /**
         * Query to find the UNIQUE key column name for a given table. This query
         * is restricted to the current schema.
         */
        public static final String QUERY_UNIQUE_KEY_FOR_TABLE =
                "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS "
                        + " WHERE TABLE_NAME = ? "
                        + " AND TABLE_SCHEMA = ? "
                        + " AND COLUMN_KEY = 'UNI'";

        /**
         * Query to find the INDEXED column name for a given table. This query
         * is restricted to the current schema.
         */
        public static final String QUERY_INDEXED_COL_FOR_TABLE =
                "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS "
                        + " WHERE TABLE_NAME = ? "
                        + " AND TABLE_SCHEMA = ? "
                        + " AND COLUMN_KEY = 'MUL'";


    }

    public static class OracleManager {
        /**
         * Query to find the primary key column name for a given table. This query
         * is restricted to the current schema.
         */
        public static final String QUERY_PRIMARY_KEY_FOR_TABLE =
                "SELECT ALL_CONS_COLUMNS.COLUMN_NAME FROM ALL_CONS_COLUMNS, "
                        + " ALL_CONSTRAINTS WHERE ALL_CONS_COLUMNS.CONSTRAINT_NAME = "
                        + " ALL_CONSTRAINTS.CONSTRAINT_NAME AND "
                        + " ALL_CONSTRAINTS.CONSTRAINT_TYPE = 'P' AND "
                        + " ALL_CONS_COLUMNS.TABLE_NAME = ? AND "
                        + " ALL_CONS_COLUMNS.OWNER = ?";

        /**
         * Query to find the UNIQUE key column name for a given table. This query
         * is restricted to the current schema.
         */
        public static final String QUERY_UNIQUE_KEY_FOR_TABLE =
                "SELECT c.COLUMN_NAME FROM ALL_IND_COLUMNS c, ALL_INDEXES i, ALL_TAB_COLUMNS t WHERE "
                        + " c.TABLE_NAME = ? and c.TABLE_OWNER = ? "
                        + " AND c.INDEX_NAME = i.INDEX_NAME AND c.TABLE_OWNER = i.TABLE_OWNER AND c.TABLE_NAME = i.TABLE_NAME "
                        + " AND i.UNIQUENESS = 'UNIQUE' "
                        + " AND c.TABLE_OWNER = t.OWNER AND c.TABLE_NAME = t.TABLE_NAME and c.COLUMN_NAME = t.COLUMN_NAME";

        /**
         * Query to find the INDEXED column name for a given table. This query
         * is restricted to the current schema.
         */
        public static final String QUERY_INDEXED_COL_FOR_TABLE =
                "SELECT c.COLUMN_NAME FROM ALL_IND_COLUMNS c, ALL_INDEXES i, ALL_TAB_COLUMNS t WHERE "
                        + " c.TABLE_NAME = ? and c.TABLE_OWNER = ? "
                        + " AND c.INDEX_NAME = i.INDEX_NAME AND c.TABLE_OWNER = i.TABLE_OWNER AND c.TABLE_NAME = i.TABLE_NAME "
                        + " AND i.UNIQUENESS = 'NONUNIQUE' "
                        + " AND c.TABLE_OWNER = t.OWNER AND c.TABLE_NAME = t.TABLE_NAME and c.COLUMN_NAME = t.COLUMN_NAME";

    }

    public static class DB2Manager {
        /**
         * Query to find the primary key column name for a given table. This query
         * is restricted to the current schema.
         */
        public static final String QUERY_PRIMARY_KEY_FOR_TABLE =
                "SELECT COLNAMES AS COLUMN_NAME FROM SYSCAT.INDEXES WHERE TABNAME = ? AND TABSCHEMA = ? AND UNIQUERULE = 'P'";

        /**
         * Query to find the UNIQUE key column name for a given table. This query
         * is restricted to the current schema.
         */
        public static final String QUERY_UNIQUE_KEY_FOR_TABLE =
                "SELECT COLNAMES AS COLUMN_NAME FROM SYSCAT.INDEXES WHERE TABNAME = ? AND TABSCHEMA = ? AND UNIQUERULE = 'U'";

        /**
         * Query to find the INDEXED column name for a given table. This query
         * is restricted to the current schema.
         */
        public static final String QUERY_INDEXED_COL_FOR_TABLE =
                "SELECT COLNAMES AS COLUMN_NAME FROM SYSCAT.INDEXES WHERE TABNAME = ? AND TABSCHEMA = ? AND UNIQUERULE = 'D'";

    }
}
