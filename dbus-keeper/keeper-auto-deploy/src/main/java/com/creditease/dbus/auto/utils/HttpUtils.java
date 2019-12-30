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

import com.alibaba.fastjson.JSON;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.util.HashMap;

public class HttpUtils {

    public static String httpGet(String address) {
        HttpURLConnection connection = null;
        try {
            URL url = new URL(address);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestProperty("Content-Type", "application/json;charset=UTF-8");
            connection.setConnectTimeout(5000);
            int code = connection.getResponseCode();
            if (code != 200) {
                return "error";
            }
            return "200";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    public static String httpPost(String address, HashMap<String, String> param) {
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

    public static void checkUrl(String host, int port) {
        Socket socket = null;
        try {
            socket = new Socket();
            socket.connect(new InetSocketAddress(host, port));
            socket.close();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    System.out.println(e.getMessage());
                }
            }
        }
    }

}
