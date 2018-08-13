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

package com.creditease.dbus.stattools;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.StatMessage;
import com.creditease.dbus.tools.common.ConfUtils;
import org.apache.http.HttpEntity;
import org.apache.http.annotation.ThreadSafe;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;

import javax.xml.bind.PropertyException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;

/**
 * Created by dongwang47 on 2016/9/2.
 */
@Deprecated
public class InfluxSink {
    private static Logger logger = LoggerFactory.getLogger(InfluxSink.class);
    private final static String CONFIG_PROPERTIES ="StatTools/config.properties";

    private String tableName = null;
    private String postURL = null;
    private URI uri = null;
    private HttpClient client = null;
    private HttpPost post = null;

    final private int CUSTOM_TIME_OUT = 1000; // 超时时间为1000毫秒

    public InfluxSink() throws IOException, PropertyException {
        Properties configProps = ConfUtils.getProps(CONFIG_PROPERTIES);
        String dbURL = configProps.getProperty(Constants.InfluxDB.DB_URL);
        String dbName = configProps.getProperty(Constants.InfluxDB.DB_NAME);
        tableName = configProps.getProperty(Constants.InfluxDB.TABLE_NAME);
        if (dbURL == null) {
            throw new PropertyException("配置参数文件内容不能为空！ " + Constants.InfluxDB.DB_URL);
        }
        if (dbName == null) {
            throw new PropertyException("配置参数文件内容不能为空！ " + Constants.InfluxDB.DB_NAME);
        }
        if (tableName == null) {
            throw new PropertyException("配置参数文件内容不能为空！ " + Constants.InfluxDB.TABLE_NAME);
        }


        postURL = String.format("%s/write?db=%s", dbURL, dbName);
        try {
            uri = new URI(postURL);
        } catch (URISyntaxException e) {
            e.printStackTrace();
            logger.warn(String.format("URI不正确，URI=%s",postURL));
        }
        client = HttpClientBuilder.create().build();
        post = new HttpPost();
        post.addHeader("Connection", "Keep-Alive");
    }

    private String statMessageToLineProtocol(StatMessage msg) {
        String fullSchemaName = msg.getDsName() + "." + msg.getSchemaName();
        String fullTableName = fullSchemaName + "." + msg.getTableName();
        String keys = String.format("type=%s,ds=%s,schema=%s,table=%s", msg.getType(),
                msg.getDsName(), fullSchemaName, fullTableName);

        String fields = String.format("count=%d,errorCount=%d,warningCount=%d,latency=%f",
                msg.getCount(), msg.getErrorCount(), msg.getWarningCount(),((float)msg.getLatencyMS())/1000);

        //time should by Nanoseconds
        long timestamp = msg.getTxTimeMS() * 1000000;

        return String.format ("%s,%s %s %d", tableName, keys, fields, timestamp);
    }

    public int sendMessage(StatMessage msg,  long retryTimes) {
        String content = null;
        HttpResponse response = null;
        int code = 0;
        try {
            post.setURI(uri);

            // add header
            content = statMessageToLineProtocol(msg);
            post.setEntity(new StringEntity(content));
            post.setConfig(RequestConfig.custom().setConnectionRequestTimeout(CUSTOM_TIME_OUT).setConnectTimeout(CUSTOM_TIME_OUT).setSocketTimeout(CUSTOM_TIME_OUT).build());
            response = client.execute(post);

            code = response.getStatusLine().getStatusCode();

            if (code == 200 || code == 204) {
                logger.info(String.format("Sink to influxdb OK! http_code=%d, content=%s", code, content));
                return 0;
            } else {
                logger.warn(String.format("http_code=%d! try %d times -- Sink to influxdb failed! url=%s, content=%s",
                        code, retryTimes, postURL, content));
            }
        } catch (Exception e) {
            logger.warn(String.format("Reason:%s. try %d times -- Sink to influxdb failed! url=%s, content=%s",
                    e.getMessage(), retryTimes, postURL, content));
        }

        // uri, client, post在post失败后重新生成
        try {
            uri = new URI(postURL);
        } catch (URISyntaxException e) {
            e.printStackTrace();
            logger.warn(String.format("URI不正确，URI=%s",postURL));
        }
        client = HttpClientBuilder.create().build();
        post = new HttpPost();
        post.addHeader("Connection", "Keep-Alive");

        logger.warn("HttpPost失败, 返回值code={},重新生成: uri={}, post={}, client={}", code, uri, post, client);

        return -1;
    }

    public int sendBatchMessages(List<StatMessage> list, long retryTimes) throws IOException {
        int ret;
        for (StatMessage msg : list) {
            ret = sendMessage(msg, retryTimes);
            if (ret != 0) {
                return ret;
            }
        }

        return 0;
    }

    public void cleanUp() {
        ;
    }
}
