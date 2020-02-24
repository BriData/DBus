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
import com.creditease.dbus.auto.utils.CmdUtils;
import com.creditease.dbus.auto.utils.ConfigUtils;
import com.creditease.dbus.auto.utils.HttpUtils;
import com.creditease.dbus.auto.utils.JdbcUtils;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.ZkService;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class DBusKeeperInitAll {

    private static String pubKeyPath;

    public static void main(String[] args) {
        try {
            Properties pro = ConfigUtils.loadConfig();
            checkConfig(pro);
            checkSSH(pro);
            initDbusMgr(pro);
            InitDBusInitNode(pro);
            DBusKeeperInitJars.initLibJars(pro);
            restartAll(pro);
            testLogin(pro);
            DBusKeeperInitDbusModules.initOthers(pro);
            System.out.println("初始化完成,请登录web开启dbus之旅.");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    private static void checkConfig(Properties pro) {
        checkConfigServer(pro);
        checkRegisterServer(pro);
        checkConfigs(pro);
        checkNginx(pro);
        checkDatabase(pro);
        checkKafka(pro);
        checInflux(pro);
        checkZK(pro);
    }

    private static void checkSSH(Properties pro) {
        checkPubKeyPath(pro);
        String clusterList = pro.getProperty("dbus.cluster.server.list");
        String clusterUser = pro.getProperty("dbus.cluster.server.ssh.user");
        String clusterPort = pro.getProperty("dbus.cluster.server.ssh.port");
        List<String> clusters = Arrays.asList(StringUtils.split(clusterList, ","));
        for (String cluster : clusters) {
            checkSSH(cluster, clusterUser, Integer.parseInt(clusterPort));
        }
    }

    private static void InitDBusInitNode(Properties pro) {
        BufferedReader br = null;
        ZkService zk = null;
        try {
            zk = new ZkService(pro.getProperty("zk.str"), 5000);
            if (!zk.isExists("/DBusInit")) {
                zk.createNode("/DBusInit", null);
            }
            br = new BufferedReader(new FileReader(new File("../conf/config.properties")));
            StringBuilder sb = new StringBuilder();
            String line = null;
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }
            zk.setData("/DBusInit", sb.toString().getBytes());
            zk.close();
            System.out.println("初始化DBusInit节点完成");
        } catch (Exception e) {
            throw new RuntimeException("无法连接ZK,请检查zk.str配置." + e.getMessage());
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
                if (zk != null) {
                    zk.close();
                }
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    private static void initDbusMgr(Properties pro) {
        System.out.println("开始初始化dbusmgr相关表...");
        Connection conn = null;
        try {
            String url = pro.getProperty("spring.datasource.url");
            String user = pro.getProperty("spring.datasource.username");
            String password = pro.getProperty("spring.datasource.password");
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url, user, password);

            List<String> sqls = JdbcUtils.readSqlsFromFile(SystemUtils.USER_DIR.replaceAll("\\\\", "/") + "/../conf/init/dbus_mgr.sql");
            JdbcUtils.batchDate(conn, sqls);
            System.out.println("初始化dbusmgr相关表成功");
        } catch (Exception e) {
            throw new RuntimeException("无法连接数据库,请检查数据库配置." + e.getMessage());
        } finally {
            try {
                if (conn != null) conn.close();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    private static void testLogin(Properties pro) {
        String url = "http://localhost:" + pro.getProperty("mgr.server.port") + "/auth/login";
        HashMap<String, String> param = new HashMap<>();
        param.put("email", "admin");
        param.put("password", "12345678");
        boolean run = true;
        while (run) {
            System.out.println("登陆测试中...");
            sleep(20000);
            String result = HttpUtils.httpPost(url, param);
            JSONObject json = JSON.parseObject(result);
            if (json.getInteger("status") == 0) {
                run = false;
            }
        }
        System.out.println("登陆测试成功.");
    }

    private static void restartAll(Properties pro) throws Exception {
        CmdUtils.executeNormalCmd("./stop.sh");
        sleep(3000);
        System.out.println("删除logs目录...");
        CmdUtils.executeNormalCmd("rm -rf ../logs");
        System.out.println("新建logs目录...");
        CmdUtils.executeNormalCmd("mkdir -p ../logs");

        boolean config = false;
        boolean register = false;
        boolean gateway = false;
        boolean mgr = false;
        boolean service = false;

        boolean configEnabled = true;
        boolean registerEnabled = true;

        // 启动配置中心
        if (StringUtils.equals("true", pro.getProperty("config.server.enabled"))) {
            System.out.println("启动config-server...");
            CmdUtils.executeNormalCmd("java -jar ../lib/config-server-" + Constants.RELEASE_VERSION + ".jar >> ../logs/config.log 2>&1 &");
        } else {
            System.out.println("使用自定义配置中心: " + pro.get("spring.cloud.config.uri"));
            config = true;
            configEnabled = false;
        }

        while (true) {
            sleep(15000);
            System.out.println("启动config-server中...");

            if (configEnabled && StringUtils.isNotBlank(CmdUtils.executeNormalCmd("cat ../logs/config.log|grep ERROR"))) {
                throw new RuntimeException("启动config-server失败,请查看config日志获取失败原因.");
            }
            if (!config && StringUtils.isNotBlank(CmdUtils.executeNormalCmd("cat ../logs/config.log|grep 'Started ConfigServerApp'"))) {
                System.out.println("启动config-server成功");
                config = true;
            }
            if (config) {
                break;
            }
        }

        // 启动注册中心
        if (StringUtils.equals("true", pro.getProperty("register.server.enabled"))) {
            System.out.println("启动register-server...");
            CmdUtils.executeNormalCmd("java -jar ../lib/register-server-" + Constants.RELEASE_VERSION + ".jar >> ../logs/register.log 2>&1 &");
        } else {
            System.out.println("使用自定义注册中心: " + pro.get("register.server.url"));
            register = true;
            registerEnabled = false;
        }

        while (true) {
            sleep(15000);
            System.out.println("启动register-server中...");

            if (registerEnabled && StringUtils.isNotBlank(CmdUtils.executeNormalCmd("cat ../logs/register.log|grep ERROR"))) {
                throw new RuntimeException("启动register-server失败,请查看register日志获取失败原因.");
            }
            if (!register && StringUtils.isNotBlank(CmdUtils.executeNormalCmd("cat ../logs/register.log|grep 'Started RegistryServerApp'"))) {
                System.out.println("启动register-server成功");
                register = true;
            }

            if (config && register) {
                break;
            }
        }

        // 启动dbus相关服务
        System.out.println("启动gateway...");
        CmdUtils.executeNormalCmd("java -jar ../lib/gateway-" + Constants.RELEASE_VERSION + ".jar >> ../logs/gateway.log 2>&1 &");
        System.out.println("启动keeper-mgr...");
        CmdUtils.executeNormalCmd("java -jar ../lib/keeper-mgr-" + Constants.RELEASE_VERSION + ".jar >> ../logs/mgr.log 2>&1 &");
        System.out.println("启动keeper-service...");
        CmdUtils.executeNormalCmd("java -jar ../lib/keeper-service-" + Constants.RELEASE_VERSION + ".jar >> ../logs/service.log 2>&1 &");

        while (true) {
            sleep(15000);
            System.out.println("dbusweb程序启动中...");

            if (StringUtils.isNotBlank(CmdUtils.executeNormalCmd("cat ../logs/gateway.log|grep ERROR"))) {
                throw new RuntimeException("启动gateway失败,请查看gateway日志获取失败原因.");
            }
            if (StringUtils.isNotBlank(CmdUtils.executeNormalCmd("cat ../logs/mgr.log|grep ERROR"))) {
                throw new RuntimeException("启动keeper-mgr失败,请查看mgr日志获取失败原因.");
            }
            if (StringUtils.isNotBlank(CmdUtils.executeNormalCmd("cat ../logs/service.log|grep ERROR"))) {
                throw new RuntimeException("启动keeper-service失败,请查看service日志获取失败原因.");
            }

            if (!gateway && StringUtils.isNotBlank(CmdUtils.executeNormalCmd("cat ../logs/gateway.log|grep 'Started GatewayApp'"))) {
                System.out.println("启动gateway成功");
                gateway = true;
            }
            if (!mgr && StringUtils.isNotBlank(CmdUtils.executeNormalCmd("cat ../logs/mgr.log|grep 'Started KeeperMgrAPP'"))) {
                System.out.println("启动keeper-mgr成功");
                mgr = true;
            }
            if (!service && StringUtils.isNotBlank(CmdUtils.executeNormalCmd("cat ../logs/service.log|grep 'Started KeeperServiceAPP'"))) {
                System.out.println("启动keeper-service成功");
                service = true;
            }
            if (config && register && gateway && mgr && service) {
                System.out.println("启动dbusweb程序成功");
                break;
            }
        }

    }


    private static void sleep(int i) {
        try {
            Thread.sleep(i);
        } catch (InterruptedException e) {
        }
    }

    private static void checkRegisterServer(Properties pro) {
        if (StringUtils.equals("false", pro.getProperty("register.server.enabled"))) {
            String registerServerUrl = pro.getProperty("register.server.url");
            if (StringUtils.isBlank(registerServerUrl)) {
                throw new RuntimeException("register.server.url不能为空");
            }
            if (StringUtils.contains(registerServerUrl, "必须修改") || !StringUtils.startsWith(registerServerUrl, "http")) {
                throw new RuntimeException("register.server.url格式不正确,正确格式[http://ip:port/xxx]");
            }
        }
    }

    private static void checkConfigServer(Properties pro) {
        if (StringUtils.equals("false", pro.getProperty("config.server.enabled"))) {
            String profile = pro.getProperty("spring.cloud.config.profile");
            if (StringUtils.isBlank(profile)) {
                throw new RuntimeException("spring.cloud.config.profile环境名不能为空");
            }
            if (StringUtils.contains(profile, "必须修改")) {
                throw new RuntimeException("spring.cloud.config.profile环境名格式不正确,正确格式[dev/pro/test/release...]");
            }
            String label = pro.getProperty("spring.cloud.config.label");
            if (StringUtils.isBlank(label)) {
                throw new RuntimeException("spring.cloud.config.label分支名不能为空");
            }
            if (StringUtils.contains(label, "必须修改")) {
                throw new RuntimeException("spring.cloud.config.label环境名格式不正确,正确格式[master]");
            }
            String uri = pro.getProperty("spring.cloud.config.uri");
            if (StringUtils.isBlank(uri)) {
                throw new RuntimeException("spring.cloud.config.uri配置中心开放服务地址不能为空");
            }
            if (StringUtils.contains(uri, "必须修改")) {
                throw new RuntimeException("spring.cloud.config.uri配置中心开放服务地址格式不正确");
            }
        }
    }


    private static void checkConfigs(Properties pro) {
        String bootstrapServices = pro.getProperty("bootstrap.servers");
        if (StringUtils.isBlank(bootstrapServices)) {
            throw new RuntimeException("bootstrap.servers不能为空");
        }
        if (StringUtils.contains(bootstrapServices, "必须修改") || !StringUtils.contains(bootstrapServices, ":")) {
            throw new RuntimeException("bootstrap.servers格式不正确,正确格式[ip1:port,ip2:port]");
        }
        if (StringUtils.isBlank(pro.getProperty("bootstrap.servers.version"))) {
            throw new RuntimeException("bootstrap.servers.version不能为空");
        }
        String grafanaWebUrl = pro.getProperty("grafana.web.url");
        if (StringUtils.isBlank(grafanaWebUrl)) {
            throw new RuntimeException("grafana.web.url不能为空");
        }
        if (StringUtils.contains(grafanaWebUrl, "必须修改") || !StringUtils.contains(grafanaWebUrl, ":")
                || !StringUtils.startsWith(grafanaWebUrl, "http://")) {
            throw new RuntimeException("grafana.web.url格式不正确,正确格式[http://ip:port或者http://域名]");
        }
        String grafanaDbusUrl = pro.getProperty("grafana.dbus.url");
        if (StringUtils.isBlank(grafanaDbusUrl)) {
            throw new RuntimeException("grafana.dbus.url不能为空");
        }
        if (StringUtils.contains(grafanaDbusUrl, "必须修改") || !StringUtils.contains(grafanaDbusUrl, ":")
                || !StringUtils.startsWith(grafanaDbusUrl, "http://")) {
            throw new RuntimeException("grafana.dbus.url格式不正确,正确格式[http://ip:port],默认端口号3000");
        }
        String grafanaToken = pro.getProperty("grafana.token");
        if (StringUtils.isBlank(grafanaToken)) {
            throw new RuntimeException("grafana.token不能为空");
        }
        if (StringUtils.contains(grafanaToken, "必须修改")) {
            throw new RuntimeException("grafana.token格式不正确");
        }
        String influxdbWebUrl = pro.getProperty("influxdb.web.url");
        if (StringUtils.isBlank(influxdbWebUrl)) {
            throw new RuntimeException("influxdb.web.url不能为空");
        }
        if (StringUtils.contains(influxdbWebUrl, "必须修改") || !StringUtils.contains(influxdbWebUrl, ":")
                || !StringUtils.startsWith(influxdbWebUrl, "http://")) {
            throw new RuntimeException("influxdb.web.url格式不正确,正确格式[http://ip:port],默认端口号8086");
        }
        String influxdbDbusUrl = pro.getProperty("influxdb.dbus.url");
        if (StringUtils.isBlank(influxdbDbusUrl)) {
            throw new RuntimeException("influxdb.dbus.url不能为空");
        }
        if (StringUtils.contains(influxdbDbusUrl, "必须修改") || !StringUtils.contains(influxdbDbusUrl, ":")
                || !StringUtils.startsWith(influxdbDbusUrl, "http://")) {
            throw new RuntimeException("influxdb.dbus.url格式不正确,正确格式[http://ip:port]");
        }
        String nimbusHost = pro.getProperty("storm.nimbus.host");
        if (StringUtils.isBlank(nimbusHost)) {
            throw new RuntimeException("storm.nimbus.host不能为空");
        }
        if (StringUtils.contains(nimbusHost, "必须修改")) {
            throw new RuntimeException("storm.nimbus.host格式不正确");
        }
        String nimbusHomePath = pro.getProperty("storm.nimbus.home.path");
        if (StringUtils.isBlank(nimbusHomePath)) {
            throw new RuntimeException("storm.nimbus.home.path");
        }
        if (StringUtils.contains(nimbusHomePath, "必须修改") || !StringUtils.startsWith(nimbusHomePath, "/")) {
            throw new RuntimeException("storm.nimbus.home.path格式不正确,必须是全路径");
        }
        String nimbusLogPath = pro.getProperty("storm.nimbus.log.path");
        if (StringUtils.isBlank(nimbusLogPath)) {
            throw new RuntimeException("storm.nimbus.log.path");
        }
        if (StringUtils.contains(nimbusLogPath, "必须修改") || !StringUtils.startsWith(nimbusLogPath, "/")) {
            throw new RuntimeException("storm.nimbus.log.path格式不正确,必须是全路径");
        }
        String stormRestUrl = pro.getProperty("storm.rest.url");
        if (StringUtils.isBlank(stormRestUrl)) {
            throw new RuntimeException("storm.rest.url不能为空");
        }
        if (StringUtils.contains(stormRestUrl, "必须修改") || !StringUtils.contains(stormRestUrl, ":") || !StringUtils.startsWith(stormRestUrl, "http://")) {
            throw new RuntimeException("storm.rest.url格式不正确,正确格式[http://ip:port/api/v1]");
        }
        String stormZookeeperRoot = pro.getProperty("storm.zookeeper.root");
        if (StringUtils.isBlank(stormZookeeperRoot)) {
            throw new RuntimeException("storm.zookeeper.root不能为空");
        }
        if (StringUtils.contains(stormZookeeperRoot, "必须修改")) {
            throw new RuntimeException("storm.zookeeper.root格式不正确");
        }
        String zkStr = pro.getProperty("zk.str");
        if (StringUtils.isBlank(zkStr)) {
            throw new RuntimeException("zk.str不能为空");
        }
        if (StringUtils.contains(zkStr, "必须修改") || !StringUtils.contains(zkStr, ":")) {
            throw new RuntimeException("zk.str格式不正确,正确格式[ip1:port,ip2:port]");
        }
        String heartbeatHost = pro.getProperty("heartbeat.host");
        if (StringUtils.isBlank(heartbeatHost)) {
            throw new RuntimeException("heartbeat.host不能为空");
        }
        if (StringUtils.contains(heartbeatHost, "必须修改")) {
            throw new RuntimeException("heartbeat.host格式不正确,正确格式[ip1,ip2]");
        }
        String heartbeatPath = pro.getProperty("heartbeat.path");
        if (StringUtils.isBlank(heartbeatPath)) {
            throw new RuntimeException("heartbeat.path不能为空");
        }
        if (StringUtils.contains(heartbeatPath, "必须修改") || !StringUtils.startsWith(heartbeatPath, "/")) {
            throw new RuntimeException("heartbeat.path格式不正确,必须是全路径");
        }
        String nginxIp = pro.getProperty("nginx.ip");
        if (StringUtils.isBlank(nginxIp)) {
            throw new RuntimeException("nginx.ip不能为空");
        }
        if (StringUtils.contains(nginxIp, "必须修改")) {
            throw new RuntimeException("nginx.ip格式不正确");
        }
        String nginxPort = pro.getProperty("nginx.port");
        if (StringUtils.isBlank(nginxPort)) {
            throw new RuntimeException("nginx.port不能为空");
        }
        if (StringUtils.contains(nginxPort, "必须修改")) {
            throw new RuntimeException("nginx.port格式不正确");
        }
        String clusterList = pro.getProperty("dbus.cluster.server.list");
        if (StringUtils.isBlank(clusterList)) {
            throw new RuntimeException("dbus.cluster.server.list不能为空");
        }
        if (StringUtils.contains(clusterList, "必须修改")) {
            throw new RuntimeException("dbus.cluster.server.list格式不正确");
        }
        String clusterUser = pro.getProperty("dbus.cluster.server.ssh.user");
        if (StringUtils.isBlank(clusterUser)) {
            throw new RuntimeException("dbus.cluster.server.ssh.user不能为空");
        }
        if (StringUtils.contains(clusterUser, "必须修改")) {
            throw new RuntimeException("dbus.cluster.server.ssh.user格式不正确");
        }
        String clusterPort = pro.getProperty("dbus.cluster.server.ssh.port");
        if (StringUtils.isBlank(clusterPort)) {
            throw new RuntimeException("dbus.cluster.server.ssh.port不能为空");
        }
        if (StringUtils.contains(clusterPort, "必须修改")) {
            throw new RuntimeException("dbus.cluster.server.ssh.port格式不正确");
        }
        List<String> clusters = Arrays.asList(StringUtils.split(clusterList, ","));
        if (!clusters.contains(nimbusHost)) {
            throw new RuntimeException("storm.nimbus.host必须部署在dbus.cluster.server.list集群列表内");
        }
        for (String host : StringUtils.split(heartbeatHost, ",")) {
            if (!clusters.contains(host)) {
                throw new RuntimeException("heartbeat.host必须部署在dbus.cluster.server.list集群列表内");
            }
        }

    }

    private static void checkNginx(Properties pro) {
        try {
            String ip = pro.getProperty("nginx.ip");
            String port = pro.getProperty("nginx.port");
            String url = "http://" + ip + ":" + port;
            if (!"200".equals(HttpUtils.httpGet(url))) {
                throw new RuntimeException("nginx地址不正确");
            }
            System.out.println("nginx地址检测通过");
        } catch (Exception e) {
            throw new RuntimeException("nginx地址检测异常");
        }
    }

    private static void checInflux(Properties pro) {
        try {
            String influxdbUrl = pro.getProperty("influxdb.dbus.url");
            if (!influxdbUrl.endsWith("/")) {
                influxdbUrl = influxdbUrl + "/";
            }
            String url = influxdbUrl + "query?q=show+databases" + "&db=_internal";
            if (!"200".equals(HttpUtils.httpGet(url))) {
                throw new RuntimeException("influxdb.dbus.url配置不正确");
            }
            System.out.println("influxdb地址检测通过");
        } catch (Exception e) {
            throw new RuntimeException("influxdb地址检测异常,请检查influxdb.dbus.url配置");
        }
    }

    private static void checkKafka(Properties pro) {
        try {
            String[] split = pro.getProperty("bootstrap.servers").split(",");
            for (String s : split) {
                String[] hostPort = s.split(":");
                if (hostPort == null || hostPort.length != 2) {
                    throw new RuntimeException("bootstrap.servers配置不正确");
                }
                HttpUtils.checkUrl(hostPort[0], Integer.parseInt(hostPort[1]));
            }
            System.out.println("kafka检测通过");
        } catch (Exception e) {
            throw new RuntimeException("kafka检测异常,请检查bootstrap.servers配置");
        }
    }

    private static void checkPubKeyPath(Properties pro) {
        System.out.println("验证密钥是否存在...");
        String homePath = System.getProperty("user.home");
        String path = "~/.ssh/id_rsa".replace("~", homePath);
        File file = new File(path);
        if (file.exists() && file.isFile()) {
            pubKeyPath = path;
            System.out.println("~/.ssh/id_rsa密钥文件存在");
        } else {
            System.out.println("~/.ssh/id_rsa密钥不存在,请检查密钥路径");
            throw new RuntimeException("~/.ssh/id_rsa密钥不存在,请检查密钥路径");
        }
    }

    private static void checkDatabase(Properties pro) {
        System.out.println("测试数据库连通性...");
        Connection conn = null;
        try {
            String url = pro.getProperty("spring.datasource.url");
            String user = pro.getProperty("spring.datasource.username");
            String password = pro.getProperty("spring.datasource.password");
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url, user, password);
            System.out.println("数据库连通性测试通过");
        } catch (Exception e) {
            throw new RuntimeException("无法连接数据库,请检查数据库配置." + e.getMessage());
        } finally {
            try {
                if (conn != null) conn.close();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    private static void checkZK(Properties pro) {
        System.out.println("测试ZK连通性...");
        ZkService zk = null;
        try {
            zk = new ZkService(pro.getProperty("zk.str"), 5000);
            System.out.println("ZK连通性测试通过");
        } catch (Exception e) {
            throw new RuntimeException("无法连接ZK,请检查zk.str配置." + e.getMessage());
        } finally {
            try {
                if (zk != null) {
                    zk.close();
                }
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    public static void checkSSH(String host, String user, Integer port) {
        Session session = null;
        ChannelExec channel = null;
        InputStream es = null;
        StringBuilder errorMsg = new StringBuilder();
        try {
            JSch jsch = new JSch();
            jsch.addIdentity(pubKeyPath);

            session = jsch.getSession(user, host, port);
            session.setTimeout(30000);
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect(30000);
            channel = (ChannelExec) session.openChannel("exec");
            channel.setCommand("echo hello world");
            channel.setInputStream(null);

            es = channel.getErrStream();
            channel.connect();
            byte[] tmp = new byte[1024];
            while (es.available() > 0) {
                int i = es.read(tmp, 0, 1024);
                if (i < 0) break;
                errorMsg.append(new String(tmp, 0, i));
            }
            sleep(1000);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (channel != null) {
                    channel.disconnect();
                }
                if (session != null) {
                    session.disconnect();
                }
                if (es != null) {
                    es.close();
                }
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }
        if (StringUtils.isNotBlank(errorMsg.toString())) {
            throw new RuntimeException(host + "机器免密配置不正确.");
        }
    }

}
