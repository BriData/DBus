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


package com.creditease.dbus.heartbeat.util;

import com.alibaba.fastjson.JSON;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;

/**
 * Created by mal on 2019/5/13.
 */
public class HttpUtil {

    private static Logger logger = LoggerFactory.getLogger(HttpUtil.class);

    public static String get(String serverUrl) {
        StringBuilder resp = new StringBuilder();
        BufferedReader reader = null;
        URL url = null;
        HttpURLConnection conn = null;
        try {
            url = new URL(serverUrl);
            conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setConnectTimeout(1000 * 5);
            //if (StringUtils.isNotBlank(authorizationToken))
            //    conn.setRequestProperty("Authorization", authorizationToken);

            reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line = null;
            while ((line = reader.readLine()) != null) {
                resp.append(line).append("\n");
            }
        } catch (IOException e) {
            logger.error(MsgUtil.format("http get access url:{0} error", url), e);
        } finally {
            if (conn != null)
                conn.disconnect();
            IOUtils.closeQuietly(reader);
        }
        return resp.toString();
    }

    public static String post(String address, HashMap<String, String> param) {
        HttpURLConnection connection = null;
        BufferedWriter writer = null;
        BufferedReader reader = null;
        try {
            String result = "";
            URL url = new URL(address);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setUseCaches(false);
            connection.setRequestProperty("Content-Type", "application/json;charset=utf-8");
            connection.connect();

            writer = new BufferedWriter(new OutputStreamWriter(connection.getOutputStream(), "UTF-8"));
            writer.write(JSON.toJSONString(param));
            writer.close();

            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                String line;
                while ((line = reader.readLine()) != null) {
                    result += line;
                }
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
            IOUtils.closeQuietly(writer);
            IOUtils.closeQuietly(reader);
        }
    }

    public static void main(String[] args) {
        System.out.println(get("http://localhost:8080/flow-line-check/checkDataLine"));
    }

}
