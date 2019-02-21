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

package com.creditease.dbus.service.source;

import com.creditease.dbus.enums.DbusDatasourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class SourceFetcher {

	private Logger logger = LoggerFactory.getLogger(getClass());

	private Connection conn;

	public abstract String buildQuery(Object... args);

	public abstract String buildQuery2(Object... args);

	public abstract String buildQuery3(String name);

	public abstract String fillParameters(PreparedStatement statement, String name) throws Exception;

	public abstract String fillParameters(PreparedStatement statement, Map<String, Object> params) throws Exception;

	public int fetchSource(Map<String, Object> params) throws Exception {
		try {
			PreparedStatement statement = conn.prepareStatement(buildQuery(params));
			fillParameters(statement, params);
			ResultSet resultSet = statement.executeQuery();
			resultSet.next();
			if (params.get("dsType").toString().equals("oracle")) {
				return resultSet.getInt("COUNT(SYSDATE)");
			} else if (params.get("dsType").toString().equals("mysql")) {
				return resultSet.getInt("1");
			}
			else {
				return 0;
			}
		} finally {
			if (!conn.isClosed()) {
				conn.close();
			}
		}
	}

	public List<String> fetchTableStructure(Map<String, Object> params) throws Exception {
		try {
			if ((params.get("dsType").toString()).equals("oracle")) {
				PreparedStatement statementTable = conn.prepareStatement(buildQuery2(params));
				fillParameters(statementTable, params);
				ResultSet resultSetOracleTable = statementTable.executeQuery();
				List<String> list = new ArrayList<>();
				while (resultSetOracleTable.next()) {
					list.add(resultSetOracleTable.getString("TABLE_NAME")
							+ "/" + resultSetOracleTable.getString("COLUMN_NAME") + "  , "
							+ resultSetOracleTable.getString("DATA_TYPE") + "\n");
				}
				PreparedStatement statementProcedure = conn.prepareStatement(buildQuery3(""));
				ResultSet resultSetOracleProcedure = statementProcedure.executeQuery();
				while (resultSetOracleProcedure.next()) {
					list.add(resultSetOracleProcedure.getString("OBJECT_NAME") + "/" + "--------");
				}
				return list;
			} else if ((params.get("dsType").toString()).equals("mysql")) {
				PreparedStatement statementName = conn.prepareStatement(buildQuery2(params));
				fillParameters(statementName, params);
				ResultSet resultSetMySqlName = statementName.executeQuery();
				List<String> list = new ArrayList<>();
				while (resultSetMySqlName.next()) {
					PreparedStatement statementColumn = conn.prepareStatement(buildQuery3(resultSetMySqlName.getString("Tables_in_dbus")));
					//fillParameters(statementColumn, resultSetMySqlName.getString("Tables_in_dbus"));
					ResultSet resultSetMySqlColumn = statementColumn.executeQuery();
					while (resultSetMySqlColumn.next()) {
						//list.add(resultSetMySqlName.getString("Tables_in_dbus")+"/"+resultSetMySqlColumn.getString("COLUMN_NAME")+"    "+resultSetMySqlColumn.getString("DATA_TYPE"));
						list.add(resultSetMySqlName.getString("Tables_in_dbus") + "/"
								+ resultSetMySqlColumn.getString("Field") + "  ,  "
								+ resultSetMySqlColumn.getString("Type") + "\n");
					}
					//list.add(resultSetMySqlName.getString("Tables_in_dbus"));
				}
				return list;
			}
			else {
				return null;
			}
		} finally {
			if (!conn.isClosed()) {
				conn.close();
			}
		}
	}

	public static SourceFetcher getFetcher(Map<String, Object> params) throws Exception {
		SourceFetcher fetcher;
		switch (DbusDatasourceType.parse(params.get("dsType").toString())) {
			case MYSQL:
				Class.forName("com.mysql.jdbc.Driver");
				fetcher = new MySqlSourceFetcher();
				break;
			case ORACLE:
				Class.forName("oracle.jdbc.driver.OracleDriver");
				fetcher = new OracleSourceFetcher();
				break;
			default:
				throw new IllegalArgumentException();
		}
		Connection conn = DriverManager.getConnection(params.get("URL").toString(), params.get("user").toString(), params.get("password").toString());
		fetcher.setConnection(conn);
		return fetcher;
	}

	protected void setConnection(Connection conn) {
		this.conn = conn;
	}

	public void close() throws Exception {
		if (this.conn != null) {
			conn.close();
		}
	}

	public ArrayList<String> getSupplementalLog() throws Exception {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			String sql = "SELECT lg.OWNER,lg.TABLE_NAME FROM  dba_log_groups lg WHERE lg.LOG_GROUP_TYPE = 'ALL COLUMN LOGGING'";
			statement = conn.prepareStatement(sql);
			resultSet = statement.executeQuery();
			ArrayList<String> list = new ArrayList<>();
			while (resultSet.next()) {
				list.add(resultSet.getString("OWNER") + "." + resultSet.getString("TABLE_NAME"));
			}
			return list;
		} finally {
			if (statement != null) {
				statement.close();
			}
			if (resultSet != null) {
				resultSet.close();
			}
		}
	}

	/**
	 * @return 获取源端 DBUS 下面的 DBUS_TABLES 表信息
	 * @throws Exception
	 */
	public List<String> listTable() throws Exception {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			String sql = "SELECT * FROM DBUS_TABLES";
			statement = conn.prepareStatement(sql);
			resultSet = statement.executeQuery();

			List<String> list = new ArrayList<>();
			while (resultSet.next()) {
				list.add(resultSet.getString("OWNER") + "." + resultSet.getString("TABLE_NAME"));
			}
			return list;
		} finally {
			if (statement != null) {
				statement.close();
			}
			if (resultSet != null) {
				resultSet.close();
			}
		}
	}

	/**
	 * @param sql
	 * @throws Exception
	 */
	public void executeSql(String sql) throws Exception {
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(sql);
			statement.execute();
		} finally {
			if (statement != null) {
				statement.close();
			}
		}
	}

}
