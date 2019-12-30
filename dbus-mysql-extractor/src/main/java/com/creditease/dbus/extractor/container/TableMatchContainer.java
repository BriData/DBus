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


package com.creditease.dbus.extractor.container;

import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class TableMatchContainer {
    private static TableMatchContainer tableMatchContainer;

    private Map<String, String> map = new HashMap<String, String>();

    private TableMatchContainer() {
    }

    public static TableMatchContainer getInstance() {
        if (tableMatchContainer == null) {
            tableMatchContainer = new TableMatchContainer();
        }
        return tableMatchContainer;
    }

    public void addTableRegex(String tableRegex) {
        String[] tableRegexs = StringUtils.split(tableRegex, ",");
        for (String regex : tableRegexs) {
            String localTbl = StringUtils.substringBefore(regex.trim(), ".");
            String partitionTblRegex = StringUtils.substringAfter(regex.trim(), ".");
            map.put(localTbl, partitionTblRegex);
        }

    }

    public String getLocalTable(String table) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            //if(!table.contains(entry.getKey()))
            //	continue;
            if (table.matches(entry.getValue())) {
                return entry.getKey();
            }
        }
        return table;
    }

    public void clear() {
        map.clear();
    }

}
