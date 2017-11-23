/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2017 Bridata
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

package com.creditease.dbus.commons;

import java.util.HashMap;
import java.util.Map;

public enum SupportedMysqlDataType {
	TINYINT,BIT,SMALLINT,SMALLINT_UNSIGNED,MEDIUMINT,INT,BIGINT,INT_UNSIGNED,BIGINT_UNSIGNED,
	FLOAT,DOUBLE,DECIMAL,
	ENUM,SET,
	DATE,DATETIME,TIMESTAMP,TIME,YEAR,
	CHAR,VARCHAR,
	TINYBLOB,TINYTEXT,BLOB,TEXT,
	BINARY, VARBINARY; 
	
	//BOOL、BOOLEAN在mysql中表示的数据类型都为TINYINT
	
	private static Map<String, SupportedMysqlDataType> map;
	
	static {
        map = new HashMap<>();
        for (SupportedMysqlDataType type : SupportedMysqlDataType.values()) {
            map.put(type.name().toUpperCase(), type);
        }
    }
	
	 public static boolean isSupported(String type) {
		 type = type.toUpperCase();
		 // INT UNSIGNED,BIGINT UNSIGNED两种类型含空格，不能作为enum名称。enum名称用下划线"_"代替。需作一下转换
		 type=type.replace(" ","_");
	     boolean supported = map.containsKey(type);
	     if (supported) 
	    	 return true;
	     return false;
	 }
	 
	 public static SupportedMysqlDataType parse(String type) {
	        return map.get(type.toUpperCase());
	    }
	 
	 /**
	     * 判断是否为字符类型
	     * @param type 类型名称
	     * @return
	     */
	    public static boolean isCharacterType(String type) {
	    	SupportedMysqlDataType dataType = parse(type);
	        if (dataType == null) return false;
	        switch (dataType) {
	            case CHAR:
	                return true;
	            case VARCHAR:
	                return true;
	            default:
	                return false;
	        }
	    }
}
