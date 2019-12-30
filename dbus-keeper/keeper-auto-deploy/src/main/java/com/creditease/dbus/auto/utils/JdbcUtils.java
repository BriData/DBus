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


package com.creditease.dbus.auto.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtils {

    public static void batchDate(Connection connection, List<String> sql) {
        try {
            Statement st = connection.createStatement();
            for (String subsql : sql) {
                st.addBatch(subsql);
            }
            st.executeBatch();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static List<String> readSqlsFromFile(String filePath) throws Exception {
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
                    System.out.println(e1.getMessage());
                }
            }
        }
        return sqls;
    }

}
