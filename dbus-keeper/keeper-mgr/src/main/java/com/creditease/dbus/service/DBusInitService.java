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


package com.creditease.dbus.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.base.com.creditease.dbus.utils.RequestSender;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.constant.ServiceNames;
import com.creditease.dbus.domain.model.Sink;
import com.creditease.dbus.utils.ConfUtils;
import com.creditease.dbus.utils.HttpClientUtils;
import com.creditease.dbus.utils.JsonFormatUtils;
import com.creditease.dbus.utils.SSHUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URLEncoder;
import java.text.MessageFormat;
import java.util.*;

@Service
public class DBusInitService {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private IZkService zkService;
    @Autowired
    private Environment env;
    @Autowired
    private RequestSender sender;
    @Autowired
    private ToolSetService toolSetService;
    @Autowired
    private JarManagerService jarManagerService;
    @Autowired
    private ConfigCenterService configCenterService;

    public ResultEntity initBasicModule(LinkedHashMap<String, String> map) throws Exception {
        ResultEntity resultEntity = new ResultEntity();
        Boolean initialized = isInitialized();

        //检测配置数据是否正确
        if (checkParams(resultEntity, map).getStatus() != 0) {
            logger.error(resultEntity.getMessage());
            return resultEntity;
        }

        //初始化zk节点
        if (initZKNodes(resultEntity, map).getStatus() != 0) {
            logger.error(resultEntity.getMessage());
            return resultEntity;
        }

        //初始化心跳
        if (initHeartBeat(resultEntity, map, initialized).getStatus() != 0) {
            logger.error(resultEntity.getMessage());
            return resultEntity;
        }

        //初始化storm
        if (initStormJars(resultEntity, map, initialized).getStatus() != 0) {
            logger.error(resultEntity.getMessage());
            return resultEntity;
        }

        //模板sink添加
        if (initDefaultSink(map).getStatus() != 0) {
            logger.error(resultEntity.getMessage());
            return resultEntity;
        }

        //Influxdb初始化
        if (initInfluxdb(resultEntity, map).getStatus() != 0) {
            logger.error(resultEntity.getMessage());
            return resultEntity;
        }

        //Grafana初始化
        if (initGrafana(resultEntity, map).getStatus() != 0) {
            logger.error(resultEntity.getMessage());
            return resultEntity;
        }

        logger.info("初始化dbus基础模块完成.");
        return resultEntity;
    }

    private ResultEntity initZKNodes(ResultEntity resultEntity, LinkedHashMap<String, String> map) {
        try {
            if (!zkService.isExists(Constants.DBUS_ROOT)) {
                zkService.createNode(Constants.DBUS_ROOT, null);
            }
            if (!zkService.isExists(Constants.COMMON_ROOT)) {
                zkService.createNode(Constants.COMMON_ROOT, null);
            }
            String homePath = map.get(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOME_PATH);
            LinkedHashMap<String, String> conf = new LinkedHashMap<>();
            conf.put(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_LIST, map.get(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_LIST));
            conf.put(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_PORT, map.get(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_PORT));
            conf.put(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_USER, map.get(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_USER));
            conf.put(KeeperConstants.GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS, map.get(KeeperConstants.GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS));
            conf.put(KeeperConstants.GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS_VERSION, map.get(KeeperConstants.GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS_VERSION));
            conf.put(KeeperConstants.GLOBAL_CONF_KEY_GRAFANA_WEB_URL, map.get(KeeperConstants.GLOBAL_CONF_KEY_GRAFANA_WEB_URL));
            conf.put(KeeperConstants.GLOBAL_CONF_KEY_GRAFANA_DBUS_URL, map.get(KeeperConstants.GLOBAL_CONF_KEY_GRAFANA_DBUS_URL));
            conf.put(KeeperConstants.GLOBAL_CONF_KEY_GRAFANA_TOKEN, map.get(KeeperConstants.GLOBAL_CONF_KEY_GRAFANA_TOKEN));
            conf.put(KeeperConstants.GLOBAL_CONF_KEY_INFLUXDB_WEB_URL, map.get(KeeperConstants.GLOBAL_CONF_KEY_INFLUXDB_WEB_URL));
            conf.put(KeeperConstants.GLOBAL_CONF_KEY_INFLUXDB_DBUS_URL, map.get(KeeperConstants.GLOBAL_CONF_KEY_INFLUXDB_DBUS_URL));
            conf.put(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOST, map.get(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOST));
            conf.put(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOME_PATH, map.get(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOME_PATH));
            conf.put(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_LOG_PATH, map.get(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_LOG_PATH));
            conf.put(KeeperConstants.GLOBAL_CONF_KEY_STORM_REST_URL, map.get(KeeperConstants.GLOBAL_CONF_KEY_STORM_REST_URL));
            conf.put(KeeperConstants.GLOBAL_CONF_KEY_STORM_ZOOKEEPER_ROOT, map.get(KeeperConstants.GLOBAL_CONF_KEY_STORM_ZOOKEEPER_ROOT));
            conf.put(KeeperConstants.GLOBAL_CONF_KEY_ZK_STR, map.get(KeeperConstants.GLOBAL_CONF_KEY_ZK_STR));
            conf.put(KeeperConstants.GLOBAL_CONF_KEY_HEARTBEAT_HOST, map.get(KeeperConstants.GLOBAL_CONF_KEY_HEARTBEAT_HOST));
            conf.put(KeeperConstants.GLOBAL_CONF_KEY_HEARTBEAT_PATH, map.get(KeeperConstants.GLOBAL_CONF_KEY_HEARTBEAT_PATH));
            conf.put(KeeperConstants.GLOBAL_CONF_KEY_JARS_PATH, homePath + "/" + KeeperConstants.STORM_JAR_DIR);
            conf.put(KeeperConstants.GLOBAL_CONF_KEY_ENCODE_JARS_PATH, homePath + "/" + KeeperConstants.STORM_ENCODER_JAR_DIR);
            conf.put(KeeperConstants.GLOBAL_CONF_KEY_KEYTAB_FILE_PATH, homePath + "/" + KeeperConstants.STORM_KEYTAB_FILE_DIR);

            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, String> entry : conf.entrySet()) {
                sb.append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
            }
            if (zkService.isExists(Constants.GLOBAL_PROPERTIES_ROOT)) {
                zkService.setData(Constants.GLOBAL_PROPERTIES_ROOT, sb.toString().getBytes("utf-8"));
            } else {
                zkService.createNode(Constants.GLOBAL_PROPERTIES_ROOT, sb.toString().getBytes("utf-8"));
            }
            //初始化其他节点数据
            toolSetService.initConfig(null);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            resultEntity.setStatus(MessageCode.INIT_ZOOKEEPER_ERROR);
            resultEntity.setMessage("初始化zookeeper节点异常,errMsg:" + e.getMessage());
        }
        return resultEntity;
    }

    private ResultEntity initInfluxdb(ResultEntity resultEntity, LinkedHashMap<String, String> map) throws Exception {
        String influxdbUrl = map.get(KeeperConstants.GLOBAL_CONF_KEY_INFLUXDB_DBUS_URL);
        if (!influxdbUrl.endsWith("/")) {
            influxdbUrl = influxdbUrl + "/";
        }
        String head = influxdbUrl + "query?q=";
        String tail = "&db=_internal";
        String url = head + URLEncoder.encode("CREATE DATABASE \"dbus_stat_db\"", "utf-8") + tail;
        String result = HttpClientUtils.httpGet(url);
        if (StringUtils.equals(result, "error")) {
            resultEntity.setStatus(MessageCode.INFLUXDB_URL_ERROR);
            resultEntity.setMessage("初始化influxdb数据库异常.");
            return resultEntity;
        }

        url = head + URLEncoder.encode("CREATE USER \"dbus\" WITH PASSWORD 'dbus!@#123'", "utf-8") + tail;
        result = HttpClientUtils.httpGet(url);
        if (StringUtils.equals(result, "error")) {
            resultEntity.setStatus(MessageCode.INFLUXDB_URL_ERROR);
            resultEntity.setMessage("初始化influxdb用户异常.");
            return resultEntity;
        }

        url = head + URLEncoder.encode("ALTER RETENTION POLICY autogen ON dbus_stat_db DURATION 15d", "utf-8") + tail;
        result = HttpClientUtils.httpGet(url);
        if (StringUtils.equals(result, "error")) {
            resultEntity.setStatus(MessageCode.INFLUXDB_URL_ERROR);
            resultEntity.setMessage("初始化influxdb更新数据保留时间异常.");
            return resultEntity;
        }
        return resultEntity;
    }

    private ResultEntity initGrafana(ResultEntity resultEntity, LinkedHashMap<String, String> map) {
        try {
            String grafanaUrl = map.get(KeeperConstants.GLOBAL_CONF_KEY_GRAFANA_DBUS_URL);
            String grafanaToken = map.get(KeeperConstants.GLOBAL_CONF_KEY_GRAFANA_TOKEN);
            String influxdbUrl = map.get(KeeperConstants.GLOBAL_CONF_KEY_INFLUXDB_DBUS_URL);
            //新建data source
            grafanaToken = "Bearer " + grafanaToken;
            if (!grafanaUrl.endsWith("/")) {
                grafanaUrl = grafanaUrl + "/";
            }
            String url = grafanaUrl + "api/datasources";
            String param = "{\"name\":\"inDB\",\"type\":\"influxdb\",\"url\":\"" + influxdbUrl + "\",\"access\":\"direct\",\"jsonData\":{},\"database\":\"dbus_stat_db\",\"user\":\"dbus\",\"password\":\"dbus!@#123\"}";
            HttpClientUtils.httpPostWithAuthorization(url, grafanaToken, param);

            //导入Grafana Dashboard
            url = grafanaUrl + "api/dashboards/db";


            JSONObject templete = new JSONObject();
            templete.put("folderId", 0);
            templete.put("overwrite", true);

            String data = ConfUtils.toString("init/grafana_schema.json").replace("${DS_INDB}", "inDB");
            templete.put("dashboard", JSON.parseObject(data));
            HttpClientUtils.httpPostWithAuthorization(url, grafanaToken, templete.toJSONString());

            data = ConfUtils.toString("init/grafana_table.json").replace("${DS_INDB}", "inDB");
            templete.put("dashboard", JSON.parseObject(data));
            HttpClientUtils.httpPostWithAuthorization(url, grafanaToken, templete.toJSONString());

            data = ConfUtils.toString("init/Heartbeat_log_filebeat.json").replace("${DS_INDB}", "inDB");
            templete.put("dashboard", JSON.parseObject(data));
            HttpClientUtils.httpPostWithAuthorization(url, grafanaToken, templete.toJSONString());

            data = ConfUtils.toString("init/Heartbeat_log_flume.json").replace("${DS_INDB}", "inDB");
            templete.put("dashboard", JSON.parseObject(data));
            HttpClientUtils.httpPostWithAuthorization(url, grafanaToken, templete.toJSONString());

            data = ConfUtils.toString("init/Heartbeat_log_logstash.json").replace("${DS_INDB}", "inDB");
            templete.put("dashboard", JSON.parseObject(data));
            HttpClientUtils.httpPostWithAuthorization(url, grafanaToken, templete.toJSONString());
            logger.info("初始化grafana dashboard完成");
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            resultEntity.setStatus(MessageCode.EXCEPTION);
            resultEntity.setMessage("初始化grafana dashboard异常,errMsg:" + e.getMessage());
        }
        return resultEntity;
    }

    private ResultEntity initDefaultSink(LinkedHashMap<String, String> map) {
        String bootstrapServers = map.get(KeeperConstants.GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS);
        String bootstrapServersVersion = map.get(KeeperConstants.GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS_VERSION);
        Sink sink = new Sink();
        sink.setSinkName("example");
        sink.setSinkDesc("i'm example");
        sink.setSinkType(bootstrapServersVersion);
        sink.setUrl(bootstrapServers);
        sink.setUpdateTime(new Date());
        sink.setIsGlobal((byte) 0);//默认0:false,1:true
        return sender.post(ServiceNames.KEEPER_SERVICE, "/sinks/exampleSink", sink).getBody();
    }

    private ResultEntity initStormJars(ResultEntity resultEntity, LinkedHashMap<String, String> map, Boolean initialized) {
        try {
            String host = map.get(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOST);
            int port = Integer.parseInt(map.get(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_PORT));
            String user = map.get(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_USER);
            String pubKeyPath = env.getProperty("pubKeyPath");
            String homePath = map.get(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOME_PATH);
            String jarsPath = homePath + "/" + KeeperConstants.STORM_JAR_DIR;
            String shellPath = jarsPath + "/dbus_startTopology.sh";
            if (initialized) {
                String cmd = MessageFormat.format("rm -rf {0}", jarsPath);
                String result = SSHUtils.executeCommand(user, host, port, pubKeyPath, cmd, true);
                if (StringUtils.isNotBlank(result)) {
                    logger.warn("删除storm包异常.errMsg:{}", result);
                }
            }

            String cmd = MessageFormat.format("mkdir -pv {0}", jarsPath + "/" + KeeperConstants.RELEASE_VERSION);
            String result = SSHUtils.executeCommand(user, host, port, pubKeyPath, cmd, true);
            if (StringUtils.isNotBlank(result)) {
                resultEntity.setStatus(MessageCode.STORM_SSH_SECRET_CONFIGURATION_ERROR);
                resultEntity.setMessage("创建storm jar目录失败.errMsg:" + result);
                return resultEntity;
            }

            ResultEntity body = sender.get(ServiceNames.KEEPER_SERVICE, "/jars/initDbusJars").getBody();
            if (body.getStatus() != 0) {
                return body;
            }
            logger.info("上传storm包,脱敏包成功.");
            result = SSHUtils.uploadFile(user, host, port, pubKeyPath, ConfUtils.getParentPath() + "/dbus_startTopology.sh", jarsPath);
            if (!StringUtils.equals(result, "ok")) {
                resultEntity.setStatus(MessageCode.STORM_SSH_SECRET_CONFIGURATION_ERROR);
                resultEntity.setMessage("上传worker启动脚本失败.errMsg:" + result);
                return resultEntity;
            }
            logger.info("上传worker启动脚本成功.");

            cmd = MessageFormat.format("chmod 755 {0}", shellPath);
            result = SSHUtils.executeCommand(user, host, port, pubKeyPath, cmd, true);
            if (StringUtils.isNotBlank(result)) {
                resultEntity.setStatus(MessageCode.STORM_SSH_SECRET_CONFIGURATION_ERROR);
                resultEntity.setMessage("修改worker启动脚本权限失败.errMsg:" + result);
                return resultEntity;
            }
            logger.info("修改worker启动脚本权限成功.");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            resultEntity.setStatus(MessageCode.EXCEPTION);
            resultEntity.setMessage(e.getMessage());
            return resultEntity;
        }
        return resultEntity;
    }

    private ResultEntity initHeartBeat(ResultEntity resultEntity, LinkedHashMap<String, String> map, Boolean initialized) {
        try {
            //上传并启动心跳程序
            String[] hosts = StringUtils.split(map.get(KeeperConstants.GLOBAL_CONF_KEY_HEARTBEAT_HOST), ",");
            int port = Integer.parseInt(map.get(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_PORT));
            String user = map.get(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_USER);
            String path = map.get(KeeperConstants.GLOBAL_CONF_KEY_HEARTBEAT_PATH);
            String heartPath = path + "/" + KeeperConstants.DBUS_HEARTBEAT;
            String heartZipPath = path + "/" + KeeperConstants.DBUS_HEARTBEAT_ZIP;
            String pubKeyPath = env.getProperty("pubKeyPath");
            for (String host : hosts) {
                if (initialized) {
                    String result = SSHUtils.executeCommand(user, host, port, pubKeyPath,
                            "ps -ef | grep 'com.creditease.dbus.heartbeat.start.Start' | grep -v grep | awk '{print $2}'| xargs kill -9", false);
                    logger.info("kill心跳进程结果:{}", result);

                    String cmd = MessageFormat.format(" rm -rf {0} {1}", heartPath, heartZipPath);
                    result = SSHUtils.executeCommand(user, host, port, pubKeyPath, cmd, true);
                    if (StringUtils.isNotBlank(result)) {
                        logger.warn("删除心跳包异常.errMsg :{}", result);
                    }
                    logger.info("删除旧的心跳程序包结果:{}", result);
                }

                String cmd = MessageFormat.format(" mkdir -pv {0}", path);
                String result = SSHUtils.executeCommand(user, host, port, pubKeyPath, cmd, true);
                if (StringUtils.isNotBlank(result)) {
                    resultEntity.setStatus(MessageCode.HEARTBEAT_SSH_SECRET_CONFIGURATION_ERROR);
                    resultEntity.setMessage("新建心跳程序部署文件夹失败:errMsg+" + result);
                    return resultEntity;
                }
                logger.info("新建心跳程序部署文件夹成功");

                result = SSHUtils.uploadFile(user, host, port, pubKeyPath, ConfUtils.getParentPath() + "/../zip/" + KeeperConstants.DBUS_HEARTBEAT_ZIP, path);
                if (!StringUtils.equals(result, "ok")) {
                    resultEntity.setStatus(MessageCode.HEARTBEAT_SSH_SECRET_CONFIGURATION_ERROR);
                    resultEntity.setMessage("上传心跳程序包失败:errMsg+" + result);
                    return resultEntity;
                }
                logger.info("上传心跳程序包成功");

                cmd = MessageFormat.format("cd {0};unzip -oq " + KeeperConstants.DBUS_HEARTBEAT_ZIP, path);
                result = SSHUtils.executeCommand(user, host, port, pubKeyPath, cmd, true);
                if (StringUtils.isNotBlank(result)) {
                    resultEntity.setStatus(MessageCode.HEARTBEAT_SSH_SECRET_CONFIGURATION_ERROR);
                    resultEntity.setMessage("解压心跳程序包失败:errMsg+" + result);
                    return resultEntity;
                }
                logger.info("解压心跳程序包成功");

                cmd = MessageFormat.format("cd {0}; chmod 755 heartbeat.sh; nohup ./heartbeat.sh >/dev/null 2>&1 & ", path + "/" + KeeperConstants.DBUS_HEARTBEAT);
                result = SSHUtils.executeCommand(user, host, port, pubKeyPath, cmd, true);
                if (StringUtils.isNotBlank(result)) {
                    resultEntity.setStatus(MessageCode.HEARTBEAT_SSH_SECRET_CONFIGURATION_ERROR);
                    resultEntity.setMessage("启动心跳程序包失败:errMsg+" + result);
                    return resultEntity;
                }
                logger.info("启动心跳程序成功");
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            resultEntity.setStatus(MessageCode.HEARTBEAT_SSH_SECRET_CONFIGURATION_ERROR);
            resultEntity.setMessage("自动部署心跳异常:errMsg+" + e.getMessage());
        }
        return resultEntity;
    }

    public ResultEntity checkParams(ResultEntity resultEntity, LinkedHashMap<String, String> map) {
        //kafka检测
        String bootstrapServers = map.get(KeeperConstants.GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS);
        for (String s : StringUtils.split(bootstrapServers, ",")) {
            String[] hostPort = StringUtils.split(s, ":");
            if (!configCenterService.urlTest(hostPort[0], Integer.parseInt(hostPort[1]))) {
                resultEntity.setStatus(MessageCode.KAFKA_BOOTSTRAP_SERVERS_IS_WRONG);
                resultEntity.setMessage("bootstrap.servers配置无法访问");
                return resultEntity;
            }
        }
        logger.info("bootstrap.servers:{}测试通过", bootstrapServers);

        //Grafana检测
        String monitUrl = map.get(KeeperConstants.GLOBAL_CONF_KEY_GRAFANA_DBUS_URL);
        if (!configCenterService.urlTest(monitUrl)) {
            resultEntity.setStatus(MessageCode.MONITOR_URL_IS_WRONG);
            resultEntity.setMessage("grafana.dbus.url配置无法访问");
            return resultEntity;
        }
        String grafanaToken = "Bearer " + map.get(KeeperConstants.GLOBAL_CONF_KEY_GRAFANA_TOKEN);
        Integer code = HttpClientUtils.httpGetWithAuthorization(monitUrl, grafanaToken);
        if (code == 401) {
            resultEntity.setStatus(MessageCode.GRAFANATOKEN_IS_ERROR);
            resultEntity.setMessage("grafana.token配置不正确");
            return resultEntity;
        }
        logger.info("grafana.dbus.url:{},grafana.token:{}测试通过", monitUrl, grafanaToken);

        //storm检测
        String host = map.get(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOST);
        int port = Integer.parseInt(map.get(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_PORT));
        String user = map.get(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_USER);
        String pubKeyPath = env.getProperty("pubKeyPath");
        String result = SSHUtils.executeCommand(user, host, port, pubKeyPath, "echo hello world", true);
        if (StringUtils.isNotBlank(result)) {
            resultEntity.setStatus(MessageCode.STORM_SSH_SECRET_CONFIGURATION_ERROR);
            resultEntity.setMessage("storm免密配置不正确.errMsg:" + result);
            return resultEntity;
        }
        String cmd = String.format("ls %s/conf|grep storm.yaml", map.get(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOME_PATH));
        result = SSHUtils.executeCommand(user, host, port, pubKeyPath, cmd, false);
        if (StringUtils.isBlank(result)) {
            resultEntity.setStatus(MessageCode.STORM_HOME_PATH_ERROR);
            resultEntity.setMessage("storm.nimbus.home.path配置不正确");
            return resultEntity;
        }
        cmd = String.format("ls %s|grep nimbus.log", map.get(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_LOG_PATH));
        result = SSHUtils.executeCommand(user, host, port, pubKeyPath, cmd, false);
        if (StringUtils.isBlank(result)) {
            resultEntity.setStatus(MessageCode.STORM_HOME_PATH_ERROR);
            resultEntity.setMessage("storm.nimbus.log.path配置不正确");
            return resultEntity;
        }
        logger.info("storm.nimbus.host:{},storm.nimbus.port:{}storm.nimbus.user:{},storm.nimbus.home.path:{},storm.nimbus.log.path:{}," +
                "测试通过", host, port, user, map.get(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOME_PATH), map.get(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_LOG_PATH));

        //Influxdb检测
        String influxdbUrl = map.get(KeeperConstants.GLOBAL_CONF_KEY_INFLUXDB_DBUS_URL);
        if (!influxdbUrl.endsWith("/")) {
            influxdbUrl = influxdbUrl + "/";
        }
        String url = influxdbUrl + "query?q=show+databases" + "&db=_internal";
        if (!"200".equals(HttpClientUtils.httpGetForCode(url))) {
            resultEntity.setStatus(MessageCode.INFLUXDB_URL_ERROR);
            resultEntity.setMessage("influxdb.dbus.url配置不正确");
            return resultEntity;
        }
        logger.info("influxdb.dbus.url:{}测试通过", influxdbUrl);

        //心跳检测
        String heartHost = map.get(KeeperConstants.GLOBAL_CONF_KEY_HEARTBEAT_HOST);
        int heartPort = Integer.parseInt(map.get(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_PORT));
        String heartUser = map.get(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_USER);
        for (String hearthost : StringUtils.split(heartHost, ",")) {
            result = SSHUtils.executeCommand(heartUser, hearthost, heartPort, pubKeyPath, "echo hello world", true);
            if (StringUtils.isNotBlank(result)) {
                resultEntity.setStatus(MessageCode.HEARTBEAT_SSH_SECRET_CONFIGURATION_ERROR);
                resultEntity.setMessage("心跳免密配置不正确:errMsg:" + result);
                return resultEntity;
            }
        }
        logger.info("heartbeat.host:{},heartbeat.port:{},heartbeat.user:{}测试通过", heartHost, heartPort, heartUser);
        return resultEntity;
    }

    public Boolean isInitialized() throws Exception {
        return zkService.isExists(Constants.DBUS_ROOT);
    }

    public ResultEntity initBasicModuleByOption(LinkedHashMap<String, String> map, String options) throws Exception {
        ResultEntity resultEntity = new ResultEntity();
        //检测配置数据是否正确
        if (checkParams(resultEntity, map).getStatus() != 0) {
            logger.error(resultEntity.getMessage());
            return resultEntity;
        }
        List<String> optionList = Arrays.asList(options.split("-"));
        if (optionList.contains("grafana")) {
            if (initGrafana(resultEntity, map).getStatus() != 0) {
                logger.error(resultEntity.getMessage());
                return resultEntity;
            }
        }
        if (optionList.contains("influxdb")) {
            if (initInfluxdb(resultEntity, map).getStatus() != 0) {
                logger.error(resultEntity.getMessage());
                return resultEntity;
            }
        }
        if (optionList.contains("storm")) {
            if (initStormJars(resultEntity, map, true).getStatus() != 0) {
                logger.error(resultEntity.getMessage());
                return resultEntity;
            }
        }
        if (optionList.contains("heartBeat")) {
            if (initHeartBeat(resultEntity, map, true).getStatus() != 0) {
                logger.error(resultEntity.getMessage());
                return resultEntity;
            }
        }
        if (optionList.contains("zk")) {
            if (initZKNodes(resultEntity, map).getStatus() != 0) {
                logger.error(resultEntity.getMessage());
                return resultEntity;
            }
        }
        return resultEntity;
    }

    public static void main(String[] args) throws Exception {
        String encode = URLEncoder.encode("CREATE USER \"dbus\" WITH PASSWORD 'dbus!@#123'", "UTF-8");
        System.out.println(encode);
    }
}
