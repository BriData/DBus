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


package com.creditease.dbus.auto.deploy;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.auto.utils.ConfigUtils;
import com.creditease.dbus.auto.utils.HttpUtils;

import java.util.LinkedHashMap;
import java.util.Properties;

public class DBusKeeperInitDbusModules {

    public static void main(String[] args) {
        try {
            Properties pro = ConfigUtils.loadConfig();
            initOthers(pro);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    public static void initOthers(Properties pro) {
        System.out.println("初始化dbus其他配置...");
        String url = "http://localhost:" + pro.getProperty("mgr.server.port") + "/init/initBasicModule";
        LinkedHashMap<String, String> param = new LinkedHashMap<>();
        param.put("dbus.cluster.server.list", pro.getProperty("dbus.cluster.server.list"));
        param.put("dbus.cluster.server.ssh.user", pro.getProperty("dbus.cluster.server.ssh.user"));
        param.put("dbus.cluster.server.ssh.port", pro.getProperty("dbus.cluster.server.ssh.port"));
        param.put("bootstrap.servers", pro.getProperty("bootstrap.servers"));
        param.put("bootstrap.servers.version", pro.getProperty("bootstrap.servers.version"));
        param.put("grafana.web.url", pro.getProperty("grafana.web.url"));
        param.put("grafana.dbus.url", pro.getProperty("grafana.dbus.url"));
        param.put("grafana.token", pro.getProperty("grafana.token"));
        param.put("influxdb.web.url", pro.getProperty("influxdb.web.url"));
        param.put("influxdb.dbus.url", pro.getProperty("influxdb.dbus.url"));
        param.put("storm.nimbus.host", pro.getProperty("storm.nimbus.host"));
        param.put("storm.nimbus.home.path", pro.getProperty("storm.nimbus.home.path"));
        param.put("storm.nimbus.log.path", pro.getProperty("storm.nimbus.log.path"));
        param.put("storm.rest.url", pro.getProperty("storm.rest.url"));
        param.put("storm.zookeeper.root", pro.getProperty("storm.zookeeper.root"));
        param.put("zk.str", pro.getProperty("zk.str"));
        param.put("heartbeat.host", pro.getProperty("heartbeat.host"));
        param.put("heartbeat.path", pro.getProperty("heartbeat.path"));

        String result = HttpUtils.httpPost(url, param);
        System.out.println(result);
        JSONObject json = JSON.parseObject(result);
        if (json.getInteger("status") != 0) {
            throw new RuntimeException(json.getString("message"));
        }
        System.out.println("初始化其他配置成功.");
    }
}
