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

package com.creditease.dbus.common;

import java.io.IOException;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpRequest {
    private Logger log = LoggerFactory.getLogger(getClass());

    private static HttpRequest httpRequst = null;

    private HttpRequest() {
    }

    public static HttpRequest getInstance() {
        if (httpRequst == null) {
            synchronized (HttpRequest.class) {
                if (httpRequst == null) {
                    httpRequst = new HttpRequest();
                }
            }
        }
        return httpRequst;
    }

    public String doGet(String url, String charset) {
        String resStr = null;
        HttpClient htpClient = new HttpClient();
        GetMethod getMethod = new GetMethod(url);
        getMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
                new DefaultHttpMethodRetryHandler());
        try {
            int statusCode = htpClient.executeMethod(getMethod);
            // log.info(statusCode);
            if (statusCode != HttpStatus.SC_OK) {
                log.error("Method failed: " + getMethod.getStatusLine());
                return resStr;
            }
            byte[] responseBody = getMethod.getResponseBody();
            resStr = new String(responseBody, charset);
        } catch (HttpException e) {
            log.error("Please check your provided http address!"); // 发生致命的异常，可能是协议不对或者返回的内容有问题
        } catch (IOException e) {
            log.error("Network anomaly"); // 发生网络异常
        } finally {
            getMethod.releaseConnection(); // 释放连接
        }
        return resStr;
    }

    public String doPost(String url, String charset, String jsonObj) {
        String resStr = null;
        HttpClient htpClient = new HttpClient();
        PostMethod postMethod = new PostMethod(url);
        postMethod.getParams().setParameter(
                HttpMethodParams.HTTP_CONTENT_CHARSET, charset);
        try {
            postMethod.setRequestEntity(new StringRequestEntity(jsonObj,
                    "application/json", charset));
            int statusCode = htpClient.executeMethod(postMethod);
            if (statusCode != HttpStatus.SC_OK) {
                // post和put不能自动处理转发 301：永久重定向，告诉客户端以后应从新地址访问 302：Moved
                if (statusCode == HttpStatus.SC_MOVED_PERMANENTLY
                        || statusCode == HttpStatus.SC_MOVED_TEMPORARILY) {
                    Header locationHeader = postMethod
                            .getResponseHeader("location");
                    String location = null;
                    if (locationHeader != null) {
                        location = locationHeader.getValue();
                        log.info("The page was redirected to :" + location);
                    } else {
                        log.info("Location field value is null");
                    }
                } else {
                    log.error("Method failed: " + postMethod.getStatusLine());
                }
                return resStr;
            }
            byte[] responseBody = postMethod.getResponseBody();
            resStr = new String(responseBody, charset);
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        } finally {
            postMethod.releaseConnection();
        }
        return resStr;
    }

    public String doPut(String url, String charset, String jsonObj) {
        String resStr = null;
        HttpClient htpClient = new HttpClient();
        PutMethod putMethod = new PutMethod(url);
        putMethod.getParams().setParameter(
                HttpMethodParams.HTTP_CONTENT_CHARSET, charset);
        try {
            putMethod.setRequestEntity(new StringRequestEntity(jsonObj,
                    "application/json", charset));
            int statusCode = htpClient.executeMethod(putMethod);
            if (statusCode != HttpStatus.SC_OK) {
                log.error("Method failed: " + putMethod.getStatusLine());
                return null;
            }
            byte[] responseBody = putMethod.getResponseBody();
            resStr = new String(responseBody, charset);
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        } finally {
            putMethod.releaseConnection();
        }
        return resStr;
    }
}
