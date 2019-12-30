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


package com.creditease.dbus.tools;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class AlterSqlTool {

    public static void main(String[] args) throws Exception {
//        List<String> sql = getAlterSql("d://2018-08-10.txt", "d://2019-12-22.txt");
        List<String> sql = getAlterSql("d://2019-02-21.txt", "d://2019-12-22.txt");
        sql.forEach(s -> System.out.println(s));
    }

    public static List<String> getAlterSql(String oldSqlPath, String newSqlPath) throws Exception {
        List<String> result = new ArrayList<>();
        Map<String, List<String>> oldSql = getSqls(oldSqlPath);
        Map<String, List<String>> newSql = getSqls(newSqlPath);
        for (Map.Entry<String, List<String>> entry : newSql.entrySet()) {
            String table = entry.getKey();
            List<String> tableSql = entry.getValue();
            List<String> oldTableSql = oldSql.get(table);
            if (oldTableSql == null) {
                String sql = createTable(table, tableSql);
                result.add(sql);
            } else {
                String sql = alterTable(table, tableSql, oldTableSql);
                if (sql != null) {
                    result.add(sql);
                }
            }
        }
        for (String table : oldSql.keySet()) {
            if (!newSql.containsKey(table)) {
                result.add("DROP TABLE `" + table + "`;");
            }
        }
        Collections.sort(result);
        return result;
    }

    private static String alterTable(String table, List<String> tableSql, List<String> oldTableSql) {
        Map<String, String> oldSql = new LinkedHashMap<>();
        int count = 0;
        for (String s : oldTableSql) {
            if (s.trim().startsWith("`")) {
                oldSql.put(s.substring(s.indexOf("`") + 1, s.lastIndexOf("`")), s.substring(s.lastIndexOf("`") + 1));
            } else {
                count++;
                oldSql.put(count + "", s);
            }
        }

        Map<String, String> newSql = new LinkedHashMap<>();
        int count1 = 0;
        for (String s : tableSql) {
            if (s.trim().startsWith("`")) {
                newSql.put(s.substring(s.indexOf("`") + 1, s.lastIndexOf("`")), s);
            } else {
                count1++;
                newSql.put(count1 + "", s);
            }
        }
        // ALTER TABLE `t_data_schema`
        StringBuilder result = new StringBuilder("ALTER TABLE `" + table + "`\n");
        for (Map.Entry<String, String> entry : newSql.entrySet()) {
            String column = entry.getKey();
            String line = entry.getValue();
            // 非索引列
            if (!column.matches("\\d+")) {
                if (line.endsWith(";")) {
                    line = line.substring(0, line.length() - 1) + ",";
                }
                // 新增列
                // ADD COLUMN `is_open`  int(1) NULL DEFAULT 0 COMMENT 'mongo是否展开节点,0不展开,1一级展开' AFTER `fullpull_split_style`,
                if (oldSql.get(column) == null) {
                    result.append("ADD COLUMN").append(line).append("\n");
                    continue;
                }
                // MODIFY COLUMN `schema_name`  varchar(65) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'schema' AFTER `ds_id`;
                // 变更列
                if (!equals(line.substring(line.lastIndexOf("`") + 1), oldSql.get(column))) {
                    result.append("MODIFY COLUMN").append(line).append("\n");
                }
            } else if (line.trim().startsWith(")")) { // 结束语
                continue;
            } else {//索引
                boolean f = true;
                for (Map.Entry<String, String> e : oldSql.entrySet()) {
                    if (e.getKey().matches("\\d+") && equals(line, e.getValue())) {
                        f = false;
                    }
                }
                if (f) {
                    result.append("ADD").append(line.replace("KEY", "INDEX")).append("\n");
                }
            }
        }
        if (result.toString().equals("ALTER TABLE `" + table + "`\n")) {
            return null;
        }

        for (String k : oldSql.keySet()) {
            if (!newSql.containsKey(k)) {
                // 索引特殊处理
                if (k.matches("\\d+")) {
                    String s = oldSql.get(k);
                    result.append("DROP INDEX  `" + s.substring(s.indexOf("`" + 1, s.lastIndexOf("`"))) + "`").append(",\n");
                    continue;
                }
                result.append("DROP COLUMN  `" + k + "`").append(",\n");
            }
        }

        String trim = result.toString().trim();
        if (trim.endsWith(",")) {
            trim = trim.substring(0, trim.length() - 1);
        }
        trim += ";\n";

        return trim;
    }

    private static String createTable(String table, List<String> tableSql) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE `" + table + "` (\n");
        tableSql.forEach(s -> sb.append(s).append("\n"));
        return sb.toString();
    }

    public static Map<String, List<String>> getSqls(String sqlPath) throws Exception {
        HashMap<String, List<String>> result = new HashMap<>();
        List<String> sqls = null;
        String table;
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(sqlPath), "UTF-8"));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().equals("") || line.trim().startsWith("--") || line.contains("DROP TABLE")) {
                    continue;
                }
                if (line.startsWith("CREATE TABLE")) {
                    sqls = new ArrayList<>();
                    table = line.substring(line.indexOf("`") + 1, line.lastIndexOf("`"));
                    result.put(table, sqls);
                } else {
                    sqls.add(line);
                }
            }
        } catch (IOException e) {
            throw e;
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
        return result;
    }

    public static boolean equals(String a, String b) {
        a = a.trim();
        b = b.trim();
        if (a.endsWith(",")) a = a.substring(0, a.length() - 1);
        if (b.endsWith(",")) b = b.substring(0, b.length() - 1);
        if (a.equals(b)) {
            return true;
        }
        return false;
    }
}
