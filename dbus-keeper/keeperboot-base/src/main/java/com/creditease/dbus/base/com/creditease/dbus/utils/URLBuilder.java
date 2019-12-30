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


package com.creditease.dbus.base.com.creditease.dbus.utils;

/**
 * Created by zhangyf on 2018/3/21.
 */
public class URLBuilder {
    public static final String DEFAULT_VER = "V1";
    public static final String DEFAULT_PROTOCOL = "http://";
    private String ver;
    private String protocol;
    private String serviceName;
    private String path;

    public URLBuilder() {
        this(DEFAULT_PROTOCOL, DEFAULT_VER);
    }

    public URLBuilder(String serviceName, String path) {
        this(DEFAULT_PROTOCOL, DEFAULT_VER, serviceName, path);
    }

    public URLBuilder(String protocol, String serviceVer, String serviceName, String path) {
        this.protocol = protocol;
        this.ver = serviceVer.toUpperCase();
        service(serviceName.toUpperCase());
        path(path);
    }

    public URLBuilder service(String serviceName) {
        this.serviceName = serviceName;
        return this;
    }

    public URLBuilder path(String path) {
        path = path.startsWith("/") ? path.substring(1) : path;
        this.path = path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
        return this;
    }

    public String build() {
        return build(null);
    }

    public String build(String queryStr) {
        StringBuilder sb = new StringBuilder();
        sb.append(protocol).append(serviceName).append("-").append(ver).append("/").append(path);
        if (queryStr != null && !queryStr.trim().isEmpty()) {
            queryStr = queryStr.startsWith("?") ? queryStr.substring(1) : queryStr;
            sb.append("?").append(queryStr);
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        URLBuilder builder = new URLBuilder();
        builder.service("serviceA").path("/users/queryByEmail/");
        System.out.println(builder.build("?email=frank@gmail.com"));
    }
}
