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

import java.sql.PreparedStatement;
import java.util.Map;

public class MySqlSourceFetcher extends SourceFetcher {
	@Override
	public String buildQuery(Object... args) {
		String sql = "select 1";
		return sql;
	}

	@Override
	public String buildQuery2(Object... args) {
		String sql = "show tables";
		return sql;
	}

	@Override
	public String buildQuery3(String name) {
		String sql = "show columns from  " + name;
		return sql;
	}

	public String fillParameters(PreparedStatement statement, String name) throws Exception {
		statement.setString(1, name);
		return null;
	}

	@Override
	public String fillParameters(PreparedStatement statement, Map<String, Object> params) throws Exception {
		return null;
	}


	private String get(Map<String, Object> map, String key) {
		return map.get(key).toString();
	}
}
