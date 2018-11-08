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

package com.creditease.dbus.stream.mysql.appender.protobuf;

import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.creditease.dbus.commons.SupportedMysqlDataType;
import org.apache.commons.lang.StringUtils;

import java.util.List;

public class Support {
	public static List<Column> getFinalColumns(EventType type, RowData rowData){
		List<Column> Columns = null;
		if(type == EventType.INSERT || type == EventType.UPDATE){
			Columns = rowData.getAfterColumnsList();	
		}else if(type == EventType.DELETE){
			Columns = rowData.getBeforeColumnsList();
		}
		return Columns;
	}
	
	public static boolean isSupported(Column column){
		String type = StringUtils.substringBefore(column.getMysqlType(), "(");
		return SupportedMysqlDataType.isSupported(type);
	}
	
	public static String getColumnType(Column column){
		String type = column.getMysqlType().toLowerCase();
		if(type.startsWith("int(") && type.endsWith("unsigned")) {
			return "int unsigned";
		}
		return StringUtils.substringBefore(column.getMysqlType(), "(");
	}
	
	public static long[] getColumnLengthAndPrecision(Column column){
		long[] ret = new long[2];
		String data = StringUtils.substringBetween(column.getMysqlType(), "(",")");
		String length = StringUtils.substringBefore(data, ",");
		String precision = StringUtils.substringAfter(data, ",");
		String type = StringUtils.substringBefore(column.getMysqlType(), "(").toUpperCase();
		if("SET".equals(type) || "ENUM".equals(type)){
			ret[0] = 0;
			ret[1] = 0;
		}else{
			if(StringUtils.isEmpty(length)){
				ret[0] = 0;
			}else{
				ret[0] = Long.parseLong(length);
			}
			if(StringUtils.isEmpty(precision)){
				ret[1] = 0;
			}else{
				ret[1] = Long.parseLong(precision);
			}
		}
		return ret;
	}
	
	public static boolean isSupported(String type){
		return SupportedMysqlDataType.isSupported(type);
	}
	
	public static String getOperTypeForUMS(EventType type){
		return type.name().toLowerCase().substring(0, 1);
	}
	
	public static void main(String[] args){
		String str="int";
		String length = StringUtils.substringBetween(str, "(",")");
		String precision = StringUtils.substringBefore(length, ",");
		String scale = StringUtils.substringAfter(length, ",");
	}
}
