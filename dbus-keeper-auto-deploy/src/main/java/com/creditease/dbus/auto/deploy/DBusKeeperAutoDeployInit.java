package com.creditease.dbus.auto.deploy;

import com.creditease.dbus.commons.ZkService;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.MessageFormat;
import java.util.Properties;
import java.util.Scanner;

public class DBusKeeperAutoDeployInit {
    private static Properties pro;
    public static void main(String[] args) throws Exception {
        System.out.println("加载config文件...");
        pro = new Properties();
        FileInputStream in = new FileInputStream("config.properties");
        pro.load(in);
        in.close();

        System.out.println("检查配置文件中...");
        if (!checkNginx()) return;
        if (!checkDatabase()) return;
        if (!checkSSH()) return;
        if (!checkKafka()) return;
        if (!checInflux()) return;
        if (!checkZK()) return;


        System.out.println("新建logs目录...");
        executeNormalCmd("mkdir -p logs");

        initGateway();
        initMgr();
        initService();
        initHeartBeat();


        System.out.println("初始化完成");
    }

    private static boolean checkNginx() {
        try {
            String ip = pro.getProperty("nginx.ip");
            String port = pro.getProperty("nginx.port");
            String url = "http://" + ip + ":" + port;
            if (!"200".equals(httpGet(url))) {
                System.out.println("nginx地址不正确");
                return false;
            }
            System.out.println("nginx地址检测通过");
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("nginx地址检测异常");
            return false;
        }
    }

    private static boolean checInflux() {
        try {
            String influxdbUrl = pro.getProperty("influxdb_url_web");
            String url = influxdbUrl + "/query?q=show+databases" + "&db=_internal";
            if (!"200".equals(httpGet(url))) {
                System.out.println("influxdb地址不正确");
                return false;
            }
            System.out.println("influxdb地址检测通过");
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("influxdb地址检测异常");
            return false;
        }
    }

    private static boolean checkKafka() {
        try {
            String[] split = pro.getProperty("bootstrap.servers").split(",");
            for (String s : split) {
                String[] hostPort = s.split(":");
                if (hostPort == null || hostPort.length != 2) {
                    System.out.println("kafka地址不正确");
                    return false;
                }
                boolean b = urlTest(hostPort[0], Integer.parseInt(hostPort[1]));
                if (!b) {
                    System.out.println("kafka地址不正确");
                    return false;
                }
            }
            System.out.println("kafka检测通过");
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
    private static boolean checkSSH() {
        System.out.println("验证密钥是否存在...");
        String homePath = System.getProperty("user.home");
        String path = pro.getProperty("pubKeyPath");
        path = path.replace("~", homePath);
        File file = new File(path);
        if (file.exists() && file.isFile()) {
            System.out.println("密钥文件存在");
            return true;
        } else {
            System.out.println("密钥不存在，请检查密钥路径");
            return false;
        }
    }

    private static boolean checkDatabase() {
        System.out.println("测试数据库连通性...");
        Connection conn = null;
        try {
            String driverClassName = pro.getProperty("spring.datasource.driver-class-name");
            String URL = pro.getProperty("spring.datasource.url");
            String USER = pro.getProperty("spring.datasource.username");
            String PASSWORD = pro.getProperty("spring.datasource.password");
            Class.forName(driverClassName);
            conn = DriverManager.getConnection(URL, USER, PASSWORD);
            System.out.println("数据库连通性测试完毕");
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("无法连接数据库，请检查数据库配置");
            return false;
        } finally {
            try {
                if (conn != null) conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static boolean checkZK() {
        System.out.println("测试ZK连通性，请等待5秒");
        try {
            ZkService zk = new ZkService(pro.getProperty("zk.str"), 5000);
            if (!zk.isExists("/DBusInit")) {
                zk.createNode("/DBusInit", null);
            }
            zk.setProperties("/DBusInit", pro);
            zk.close();
            System.out.println("ZK连通性测试完毕");
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("无法连接ZK，请检查ZK配置");
            return false;
        }
    }

    private static void initHeartBeat() throws Exception {
        System.out.println("解压dbus-heartbeat-0.5.0...");
        executeNormalCmd("unzip -oq dbus-heartbeat-0.5.0.zip");
        executeNormalCmd("rm -rf ./dbus-heartbeat-0.5.0/conf");
        executeNormalCmd("cp -rf ./dbus-heartbeat-0.5.0/conf_opensource ./dbus-heartbeat-0.5.0/conf");
        System.out.println("配置dbus-heartbeat-0.5.0...");
        replaceTemplate("./dbus-heartbeat-0.5.0/conf/consumer.properties");
        replaceTemplate("./dbus-heartbeat-0.5.0/conf/jdbc.properties");
        replaceTemplate("./dbus-heartbeat-0.5.0/conf/consumer.properties");
        replaceTemplate("./dbus-heartbeat-0.5.0/conf/producer.properties");
        replaceTemplate("./dbus-heartbeat-0.5.0/conf/stat_config.properties");
        replaceTemplate("./dbus-heartbeat-0.5.0/conf/zk.properties");
        System.out.println("压缩dbus-heartbeat-0.5.0...");
        executeNormalCmd("rm dbus-heartbeat-0.5.0.zip");
        executeNormalCmd("zip -qr0 dbus-heartbeat-0.5.0.zip dbus-heartbeat-0.5.0");
        executeNormalCmd("rm -r dbus-heartbeat-0.5.0");
    }

    private static void initService() throws Exception {
        System.out.println("解压keeper-service...");
        executeNormalCmd("unzip -oq lib/keeper-service-0.5.0.jar");
        executeNormalCmd("cp -f ./BOOT-INF/classes/application-opensource.yaml ./BOOT-INF/classes/application.yaml");
        System.out.println("配置keeper-service...");
        replaceTemplate("./BOOT-INF/classes/application.yaml");
        System.out.println("压缩keeper-service...");
        executeNormalCmd("rm lib/keeper-service-0.5.0.jar");
        executeNormalCmd("zip -qr0 lib/keeper-service-0.5.0.jar BOOT-INF/ META-INF/ org/");
        executeNormalCmd("rm -r BOOT-INF/ META-INF/ org/");
    }

    private static void initMgr() throws Exception {
        System.out.println("解压keeper-mgr...");
        executeNormalCmd("unzip -oq lib/keeper-mgr-0.5.0.jar");
        executeNormalCmd("cp -f ./BOOT-INF/classes/application-opensource.yaml ./BOOT-INF/classes/application.yaml");
        System.out.println("配置keeper-mgr...");
        replaceTemplate("./BOOT-INF/classes/application.yaml");
        System.out.println("压缩keeper-mgr...");
        executeNormalCmd("rm lib/keeper-mgr-0.5.0.jar");
        executeNormalCmd("zip -qr0 lib/keeper-mgr-0.5.0.jar BOOT-INF/ META-INF/ org/");
        executeNormalCmd("rm -r BOOT-INF/ META-INF/ org/");
    }

    private static void initGateway() throws Exception {
        System.out.println("解压gateway...");
        executeNormalCmd("unzip -oq lib/gateway-0.5.0.jar");
        executeNormalCmd("cp -f ./BOOT-INF/classes/application-opensource.yaml ./BOOT-INF/classes/application.yaml");
        System.out.println("配置gateway...");
        replaceTemplate("./BOOT-INF/classes/application.yaml");
        System.out.println("压缩gateway...");
        executeNormalCmd("rm lib/gateway-0.5.0.jar");
        executeNormalCmd("zip -qr0 lib/gateway-0.5.0.jar BOOT-INF/ META-INF/ org/");
        executeNormalCmd("rm -r BOOT-INF/ META-INF/ org/");
    }

    private static void replaceTemplate(String filepath) throws Exception {
        Scanner scanner = new Scanner(new File(filepath));
        StringBuilder sb = new StringBuilder();
        while (scanner.hasNextLine()) {
            String s = scanner.nextLine();
            for (String key : pro.stringPropertyNames()) {
                s = s.replace("#{" + key + "}#", pro.getProperty(key));
            }
            sb.append(s).append(System.getProperty("line.separator"));
        }
        scanner.close();
        PrintWriter printWriter = new PrintWriter(new File(filepath));
        printWriter.print(sb.toString());
        printWriter.close();
    }

    private static void executeNormalCmd(String cmd) throws Exception {
        Process ps = Runtime.getRuntime().exec(cmd);
        ps.waitFor();
//        Scanner sc = new Scanner(ps.getInputStream());
//        while(sc.hasNextLine()) System.out.println(sc.nextLine());
//        sc.close();
    }

    public static boolean urlTest(String host, int port) {
        boolean result = false;
        Socket socket = null;
        try {
            socket = new Socket();
            socket.connect(new InetSocketAddress(host, port));
            socket.close();
            result = true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return result;
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
}
