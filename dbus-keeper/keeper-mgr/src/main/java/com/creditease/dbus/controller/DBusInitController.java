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


package com.creditease.dbus.controller;

import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.service.DBusInitService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;

@RestController
@RequestMapping(value = "/init")
public class DBusInitController extends BaseController {

    @Autowired
    private DBusInitService service;

    @PostMapping(path = "/initBasicModule", consumes = "application/json")
    public ResultEntity initBasicModule(@RequestBody LinkedHashMap<String, String> map) {
        ResultEntity resultEntity = new ResultEntity(MessageCode.EXCEPTION, "");
        try {
            if (checkParams(map, resultEntity)) return resultEntity;
            return service.initBasicModule(map);
        } catch (Exception e) {
            logger.error("Exception encountered while init other configs ", e);
            resultEntity.setMessage(e.getMessage());
            return resultEntity;
        }
    }

    /**
     * 根据勾选web部分初始化
     */
    @PostMapping(path = "/initBasicModuleByOption", consumes = "application/json")
    public ResultEntity initBasicModuleByOption(@RequestBody LinkedHashMap<String, String> map, String options) {
        try {
            ResultEntity resultEntity = new ResultEntity(MessageCode.EXCEPTION, "");
            if (checkParams(map, resultEntity)) return resultEntity;
            return service.initBasicModuleByOption(map, options);
        } catch (Exception e) {
            logger.error("Exception encountered while updateBasicConfByOption ", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    private boolean checkParams(@RequestBody LinkedHashMap<String, String> map, ResultEntity resultEntity) {
        String clusterList = map.get(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_LIST);
        if (StringUtils.isBlank(clusterList)) {
            resultEntity.setMessage("dbus.cluster.server.list不能为空");
            return true;
        }
        if (StringUtils.contains(clusterList, "必须修改")) {
            resultEntity.setMessage("dbus.cluster.server.list格式不正确,默认端口22");
            return true;
        }
        String clusterPort = map.get(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_PORT);
        if (StringUtils.isBlank(clusterPort)) {
            resultEntity.setMessage("dbus.cluster.server.ssh.port不能为空");
            return true;
        }
        if (StringUtils.contains(clusterPort, "必须修改")) {
            resultEntity.setMessage("dbus.cluster.server.ssh.port格式不正确,默认端口22");
            return true;
        }
        String clusterUser = map.get(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_USER);
        if (StringUtils.isBlank(clusterUser)) {
            resultEntity.setMessage("dbus.cluster.server.ssh.user不能为空");
            return true;
        }
        if (StringUtils.contains(clusterUser, "必须修改")) {
            resultEntity.setMessage("dbus.cluster.server.ssh.user格式不正确");
            return true;
        }
        String bootstrapServices = map.get(KeeperConstants.GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS);
        if (StringUtils.isBlank(bootstrapServices)) {
            resultEntity.setMessage("bootstrap.servers不能为空");
            return true;
        }
        if (StringUtils.contains(bootstrapServices, "必须修改") || !StringUtils.contains(bootstrapServices, ":")) {
            resultEntity.setMessage("bootstrap.servers格式不正确,正确格式[ip1:port,ip2:port]");
            return true;
        }
        if (StringUtils.isBlank(map.get(KeeperConstants.GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS_VERSION))) {
            resultEntity.setMessage("bootstrap.servers.version不能为空");
            return true;
        }
        String grafanaWebUrl = map.get(KeeperConstants.GLOBAL_CONF_KEY_GRAFANA_WEB_URL);
        if (StringUtils.isBlank(grafanaWebUrl)) {
            resultEntity.setMessage("grafana.web.url不能为空");
            return true;
        }
        if (StringUtils.contains(grafanaWebUrl, "必须修改") || !StringUtils.contains(grafanaWebUrl, ":")
                || !StringUtils.startsWith(grafanaWebUrl, "http://")) {
            resultEntity.setMessage("grafana.web.url格式不正确,正确格式[http://ip:port或者http://域名]");
            return true;
        }
        String grafanaDbusUrl = map.get(KeeperConstants.GLOBAL_CONF_KEY_GRAFANA_DBUS_URL);
        if (StringUtils.isBlank(grafanaDbusUrl)) {
            resultEntity.setMessage("grafana.dbus.url不能为空");
            return true;
        }
        if (StringUtils.contains(grafanaDbusUrl, "必须修改") || !StringUtils.contains(grafanaDbusUrl, ":")
                || !StringUtils.startsWith(grafanaDbusUrl, "http://")) {
            resultEntity.setMessage("grafana.dbus.url格式不正确,正确格式[http://ip:port],默认端口号3000");
            return true;
        }
        String grafanaToken = map.get(KeeperConstants.GLOBAL_CONF_KEY_GRAFANA_TOKEN);
        if (StringUtils.isBlank(grafanaToken)) {
            resultEntity.setMessage("grafana.token不能为空");
            return true;
        }
        if (StringUtils.contains(grafanaToken, "必须修改")) {
            resultEntity.setMessage("grafana.token格式不正确");
            return true;
        }
        String influxdbWebUrl = map.get(KeeperConstants.GLOBAL_CONF_KEY_INFLUXDB_WEB_URL);
        if (StringUtils.isBlank(influxdbWebUrl)) {
            resultEntity.setMessage("influxdb.web.url不能为空");
            return true;
        }
        if (StringUtils.contains(influxdbWebUrl, "必须修改") || !StringUtils.contains(influxdbWebUrl, ":")
                || !StringUtils.startsWith(influxdbWebUrl, "http://")) {
            resultEntity.setMessage("influxdb.web.url格式不正确,正确格式[http://ip:port],默认端口号8086");
            return true;
        }
        String influxdbDbusUrl = map.get(KeeperConstants.GLOBAL_CONF_KEY_INFLUXDB_DBUS_URL);
        if (StringUtils.isBlank(influxdbDbusUrl)) {
            resultEntity.setMessage("influxdb.dbus.url不能为空");
            return true;
        }
        if (StringUtils.contains(influxdbDbusUrl, "必须修改") || !StringUtils.contains(influxdbDbusUrl, ":")
                || !StringUtils.startsWith(influxdbDbusUrl, "http://")) {
            resultEntity.setMessage("influxdb.dbus.url格式不正确,正确格式[http://ip:port]");
            return true;
        }
        String nimbusHost = map.get(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOST);
        if (StringUtils.isBlank(nimbusHost)) {
            resultEntity.setMessage("storm.nimbus.host不能为空");
            return true;
        }
        if (StringUtils.contains(nimbusHost, "必须修改")) {
            resultEntity.setMessage("storm.nimbus.host格式不正确");
            return true;
        }
        String nimbusHomePath = map.get(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOME_PATH);
        if (StringUtils.isBlank(nimbusHomePath)) {
            resultEntity.setMessage("storm.nimbus.home.path不能为空");
            return true;
        }
        if (StringUtils.contains(nimbusHomePath, "必须修改") || !StringUtils.startsWith(nimbusHomePath, "/")) {
            resultEntity.setMessage("storm.nimbus.home.path格式不正确,必须是全路径");
            return true;
        }
        String nimbusLogPath = map.get(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_LOG_PATH);
        if (StringUtils.isBlank(nimbusLogPath)) {
            resultEntity.setMessage("storm.nimbus.log.path不能为空");
            return true;
        }
        if (StringUtils.contains(nimbusLogPath, "必须修改") || !StringUtils.startsWith(nimbusLogPath, "/")) {
            resultEntity.setMessage("storm.nimbus.log.path格式不正确,必须是全路径");
            return true;
        }
        String stormRestUrl = map.get(KeeperConstants.GLOBAL_CONF_KEY_STORM_REST_URL);
        if (StringUtils.isBlank(stormRestUrl)) {
            resultEntity.setMessage("storm.rest.url不能为空");
            return true;
        }
        if (StringUtils.contains(stormRestUrl, "必须修改") || !StringUtils.contains(stormRestUrl, ":") || !StringUtils.startsWith(stormRestUrl, "http://")) {
            resultEntity.setMessage("storm.rest.url格式不正确,正确格式[http://ip:port/api/v1]");
            return true;
        }
        String stormZookeeperRoot = map.get(KeeperConstants.GLOBAL_CONF_KEY_STORM_ZOOKEEPER_ROOT);
        if (StringUtils.isBlank(stormZookeeperRoot)) {
            resultEntity.setMessage("storm.zookeeper.root不能为空");
            return true;
        }
        if (StringUtils.contains(stormZookeeperRoot, "必须修改")) {
            resultEntity.setMessage("storm.zookeeper.root格式不正确");
            return true;
        }
        String zkStr = map.get(KeeperConstants.GLOBAL_CONF_KEY_ZK_STR);
        if (StringUtils.isBlank(zkStr)) {
            resultEntity.setMessage("zk.str不能为空");
            return true;
        }
        if (StringUtils.contains(zkStr, "必须修改") || !StringUtils.contains(zkStr, ":")) {
            resultEntity.setMessage("zk.str格式不正确,正确格式[ip1:port,ip2:port]");
            return true;
        }
        String heartbeatHost = map.get(KeeperConstants.GLOBAL_CONF_KEY_HEARTBEAT_HOST);
        if (StringUtils.isBlank(heartbeatHost)) {
            resultEntity.setMessage("heartbeat.host不能为空");
            return true;
        }
        if (StringUtils.contains(heartbeatHost, "必须修改")) {
            resultEntity.setMessage("heartbeat.host格式不正确,正确格式[ip1,ip2]");
            return true;
        }
        String heartbeatPath = map.get(KeeperConstants.GLOBAL_CONF_KEY_HEARTBEAT_PATH);
        if (StringUtils.isBlank(heartbeatPath)) {
            resultEntity.setMessage("heartbeat.path不能为空");
            return true;
        }
        if (StringUtils.contains(heartbeatPath, "必须修改") || !StringUtils.startsWith(heartbeatPath, "/")) {
            resultEntity.setMessage("heartbeat.path格式不正确,必须是全路径");
            return true;
        }
        return false;
    }

}
