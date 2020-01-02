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

import com.creditease.dbus.auto.utils.CmdUtils;
import com.creditease.dbus.auto.utils.ConfigUtils;
import com.creditease.dbus.commons.Constants;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.Properties;
import java.util.Scanner;

public class DBusKeeperInitJars {

    public static void main(String[] args) {
        try {
            Properties pro = ConfigUtils.loadConfig();
            initLibJars(pro);
            System.out.println("初始化jar包完成.");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    public static void initLibJars(Properties pro) throws Exception {
        Properties properties = new Properties();
        properties.put("config.server.port", pro.get("config.server.port"));
        properties.put("spring.cloud.config.profile", "release");
        properties.put("spring.cloud.config.label", "master");
        properties.put("spring.cloud.config.uri", "http://localhost:" + pro.get("config.server.port"));
        properties.put("register.server.port", pro.get("register.server.port"));
        properties.put("spring.datasource.url", pro.get("spring.datasource.url"));
        properties.put("spring.datasource.username", pro.get("spring.datasource.username"));
        properties.put("spring.datasource.password", pro.get("spring.datasource.password"));
        properties.put("gateway.server.port", pro.get("gateway.server.port"));
        properties.put("mgr.server.port", pro.get("mgr.server.port"));
        properties.put("service.server.port", pro.get("service.server.port"));
        properties.put("zk.str", pro.get("zk.str"));

        //true: 初始化本地注册中心
        if (StringUtils.equals("true", pro.getProperty("register.server.enabled"))) {
            properties.put("register.server.url", "http://localhost:" + pro.get("register.server.port") + "/eureka/");
        } else {
            properties.put("register.server.url", pro.get("register.server.url"));
        }

        //true: 初始化本地配置中心
        if (StringUtils.equals("true", pro.getProperty("config.server.enabled"))) {
            properties.put("native.config.path", System.getProperty("user.dir") + "/../conf/keeperConfig");
            properties.put("spring.cloud.config.uri", "http://localhost:" + pro.getProperty("config.server.port"));
            initConfigJars(properties);
            //本地配置中心配置初始化
            initKeeperConfig(properties);
        } else {
            properties.put("spring.cloud.config.profile", pro.get("spring.cloud.config.profile"));
            properties.put("spring.cloud.config.label", pro.get("spring.cloud.config.label"));
            properties.put("spring.cloud.config.uri", pro.get("spring.cloud.config.uri"));
        }

        if (StringUtils.equals("true", pro.getProperty("register.server.enabled"))) {
            initKeeperJars(properties, "register-server-" + Constants.RELEASE_VERSION + ".jar", "keeper-register-bootstrap.yaml");
        }
        initKeeperJars(properties, "gateway-" + Constants.RELEASE_VERSION + ".jar", "keeper-gataway-bootstrap.yaml");
        initKeeperJars(properties, "keeper-mgr-" + Constants.RELEASE_VERSION + ".jar", "keeper-manager-bootstrap.yaml");
        initKeeperJars(properties, "keeper-service-" + Constants.RELEASE_VERSION + ".jar", "keeper-service-bootstrap.yaml");

        //初始化心跳包
        properties.put("mgr.server.port", pro.get("mgr.server.port"));
        properties.put("bootstrap.servers", pro.get("bootstrap.servers"));
        properties.put("influxdb.dbus.url", pro.get("influxdb.dbus.url"));
        initHeartBeat(properties);
    }

    private static void initConfigJars(Properties pro) throws Exception {
        String jarName = "config-server-" + Constants.RELEASE_VERSION + ".jar";
        System.out.println(String.format("解压%s...", jarName));
        executeNormalCmd(String.format("unzip -oq ../lib/%s", jarName));
        executeNormalCmd("cp -rf ../conf/keeperConfigTemplates/application.yaml BOOT-INF/classes/application.yaml");
        System.out.println(String.format("配置%s...", jarName));
        replaceTemplate(pro, "BOOT-INF/classes/application.yaml");
        System.out.println(String.format("压缩%s...", jarName));
        executeNormalCmd(String.format("rm -rf ../lib/%s", jarName));
        executeNormalCmd(String.format("zip -qr0 ../lib/%s BOOT-INF/ META-INF/ org/", jarName));
        executeNormalCmd("rm -rf BOOT-INF/ META-INF/ org/");
    }

    private static void initKeeperConfig(Properties pro) throws Exception {
        System.out.println("本地配置中心配置初始化...");
        executeNormalCmd("mkdir -pv ../conf/keeperConfig");
        executeNormalCmd("cp -rf ../conf/keeperConfigTemplates/dbus-keeper-registry-v1-release.yaml ../conf/keeperConfig/dbus-keeper-registry-v1-release.yaml");
        executeNormalCmd("cp -rf ../conf/keeperConfigTemplates/dbus-keeper-gateway-v1-release.yaml ../conf/keeperConfig/dbus-keeper-gateway-v1-release.yaml");
        executeNormalCmd("cp -rf ../conf/keeperConfigTemplates/dbus-keeper-mgr-v1-release.yaml ../conf/keeperConfig/dbus-keeper-mgr-v1-release.yaml");
        executeNormalCmd("cp -rf ../conf/keeperConfigTemplates/dbus-keeper-service-v1-release.yaml ../conf/keeperConfig/dbus-keeper-service-v1-release.yaml");
        replaceTemplate(pro, "../conf/keeperConfig/dbus-keeper-registry-v1-release.yaml");
        replaceTemplate(pro, "../conf/keeperConfig/dbus-keeper-gateway-v1-release.yaml");
        replaceTemplate(pro, "../conf/keeperConfig/dbus-keeper-mgr-v1-release.yaml");
        replaceTemplate(pro, "../conf/keeperConfig/dbus-keeper-service-v1-release.yaml");
        System.out.println("本地配置中心配置初始化完成");
    }

    private static void initHeartBeat(Properties pro) throws Exception {
        String addr = InetAddress.getLocalHost().getHostAddress();
        pro.put("dbus.mgr.server.host", addr);
        System.out.println("解压dbus-heartbeat-" + Constants.RELEASE_VERSION + "...");
        executeNormalCmd("unzip -oq ../zip/dbus-heartbeat-" + Constants.RELEASE_VERSION + ".zip");
        executeNormalCmd("cp -rf dbus-heartbeat-" + Constants.RELEASE_VERSION + "/conf_opensource dbus-heartbeat-" + Constants.RELEASE_VERSION + "/conf");
        System.out.println("配置dbus-heartbeat-" + Constants.RELEASE_VERSION + "...");
        replaceTemplate(pro, "dbus-heartbeat-" + Constants.RELEASE_VERSION + "/conf/consumer.properties");
        replaceTemplate(pro, "dbus-heartbeat-" + Constants.RELEASE_VERSION + "/conf/jdbc.properties");
        replaceTemplate(pro, "dbus-heartbeat-" + Constants.RELEASE_VERSION + "/conf/producer.properties");
        replaceTemplate(pro, "dbus-heartbeat-" + Constants.RELEASE_VERSION + "/conf/stat_config.properties");
        replaceTemplate(pro, "dbus-heartbeat-" + Constants.RELEASE_VERSION + "/conf/zk.properties");
        System.out.println("压缩dbus-heartbeat-" + Constants.RELEASE_VERSION + "...");
        executeNormalCmd("rm -rf ../zip/dbus-heartbeat-" + Constants.RELEASE_VERSION + ".zip");
        executeNormalCmd("zip -qr0 ../zip/dbus-heartbeat-" + Constants.RELEASE_VERSION + ".zip dbus-heartbeat-" + Constants.RELEASE_VERSION);
        executeNormalCmd("rm -rf dbus-heartbeat-" + Constants.RELEASE_VERSION);
    }

    private static void initKeeperJars(Properties pro, String jarName, String ymalName) throws Exception {
        System.out.println(String.format("解压%s...", jarName));
        executeNormalCmd(String.format("unzip -oq ../lib/%s", jarName));
        executeNormalCmd("cp -rf ../conf/keeperConfigTemplates/" + ymalName + " BOOT-INF/classes/bootstrap.yaml");
        System.out.println(String.format("配置%s...", jarName));
        replaceTemplate(pro, "BOOT-INF/classes/bootstrap.yaml");
        System.out.println(String.format("压缩%s...", jarName));
        executeNormalCmd(String.format("rm -rf ../lib/%s", jarName));
        executeNormalCmd(String.format("zip -qr0 ../lib/%s BOOT-INF/ META-INF/ org/", jarName));
        executeNormalCmd("rm -rf BOOT-INF/ META-INF/ org/");
    }

    private static void replaceTemplate(Properties pro, String filePath) throws Exception {
        Scanner scanner = null;
        PrintWriter printWriter = null;
        try {
            scanner = new Scanner(new File(filePath));
            StringBuilder sb = new StringBuilder();
            while (scanner.hasNextLine()) {
                String s = scanner.nextLine();
                for (String key : pro.stringPropertyNames()) {
                    s = s.replace("${" + key + "}", pro.getProperty(key));
                }
                sb.append(s).append(System.getProperty("line.separator"));
            }
            printWriter = new PrintWriter(new File(filePath));
            printWriter.print(sb.toString());
        } finally {
            if (scanner != null) {
                scanner.close();
            }
            if (printWriter != null) {
                printWriter.close();
            }
        }
    }

    private static String executeNormalCmd(String cmd) throws Exception {
        return CmdUtils.executeNormalCmd(cmd);
    }

}
