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

package com.creditease.dbus.utils;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpClientUtils {

    private static Logger logger = LoggerFactory.getLogger(HttpClientUtils.class);


    public static String httpPostWithAuthorization(String url, String authorization, String obj) {
        CloseableHttpClient httpclient = null;
        InputStream input = null;
        BufferedReader br = null;
        try {
            RequestConfig config = RequestConfig.custom().setConnectTimeout(60000).setSocketTimeout(15000).build();
            httpclient = HttpClientBuilder.create().setDefaultRequestConfig(config).build();
            HttpPost httppost = new HttpPost(url);
            httppost.addHeader("Authorization", authorization);
            httppost.setHeader("Content-Type", "application/json");
            httppost.setHeader("Accept", "application/json");
            httppost.setEntity(new StringEntity(obj, Charset.forName("UTF-8")));
            HttpResponse response = httpclient.execute(httppost);
            HttpEntity entity = response.getEntity();
            input = entity.getContent();
            br = new BufferedReader(new InputStreamReader(input));
            StringBuilder data = new StringBuilder();
            String line = "";
            while ((line = br.readLine()) != null) {
                data.append(line);
            }
            return data.toString();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return "";
        } finally {
            close(httpclient, input, br);
        }
    }

    public static Integer httpGetWithAuthorization(String url, String authorization) {
        CloseableHttpClient httpclient = null;
        InputStream input = null;
        BufferedReader br = null;
        Integer statusCode = null;
        try {
            RequestConfig config = RequestConfig.custom().setConnectTimeout(60000).setSocketTimeout(15000).build();
            httpclient = HttpClientBuilder.create().setDefaultRequestConfig(config).build();
            HttpGet httpget = new HttpGet(url);
            httpget.addHeader("Authorization", authorization);
            HttpResponse response = httpclient.execute(httpget);
            statusCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            input = entity.getContent();
            br = new BufferedReader(new InputStreamReader(input));
            StringBuilder data = new StringBuilder();
            String line = "";
            while ((line = br.readLine()) != null) {
                data.append(line);
            }
            logger.info(data.toString());
            return statusCode;
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return statusCode;
        } finally {
            close(httpclient, input, br);
        }
    }

    public static String httpGet(String s) {
        HttpURLConnection connection = null;
        try {
            URL url = new URL(s);
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

    private static void close(CloseableHttpClient httpclient, InputStream input, BufferedReader br) {
        try {
            if (httpclient != null) {
                httpclient.close();
            }
            if (input != null) {
                input.close();
            }
            if (br != null) {
                br.close();
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }


    /**
     * grafana utils
     *
     * @param serverUrl
     * @param method
     * @param param
     * @param token
     * @return
     */
    public static List<Object> send(String serverUrl, String method, String param, String token) {
        List<Object> ret = new ArrayList<>();
        ret.add(-1);

        StringBuilder response = new StringBuilder();
        BufferedReader reader = null;
        BufferedWriter writer = null;
        URL url = null;
        try {
            url = new URL(serverUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestProperty("Content-Type", "application/json;charset=UTF-8");
            conn.setRequestProperty("Authorization", "Bearer " + token);
            conn.setRequestMethod(method);
            conn.setDoInput(true);
            conn.setConnectTimeout(1000 * 5);

            if (StringUtils.isNotBlank(param)) {
                conn.setDoOutput(true);
                writer = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream()));
                writer.write(param);
                writer.flush();
            }

            int httpStatus = conn.getResponseCode();
            ret.set(0, httpStatus);

            if (httpStatus == 401 ||
                    httpStatus == 403 ||
                    httpStatus == 404) {
                if (httpStatus == 404) {
                    reader = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
                } else {
                    response.append(conn.getResponseMessage());
                }
            }
            if (httpStatus == 200) {
                reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            }

            if (reader != null) {
                String line = null;
                while ((line = reader.readLine()) != null) {
                    response.append(line).append(SystemUtils.LINE_SEPARATOR);
                }
            }
        } catch (IOException e) {
            logger.error("send request error", e);
            ret.set(0, -1);
        } finally {
            IOUtils.closeQuietly(reader);
            IOUtils.closeQuietly(writer);
        }
        ret.add(response.toString());
        return ret;
    }

}
