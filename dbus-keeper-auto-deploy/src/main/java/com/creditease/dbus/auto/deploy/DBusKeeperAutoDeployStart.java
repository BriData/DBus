package com.creditease.dbus.auto.deploy;

import com.creditease.dbus.commons.ZkService;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DBusKeeperAutoDeployStart {

    public static void main(String[] args) throws Exception {
        if (!checkInit()) return;

        String[] lines = executeNormalCmd("jps -l").split(System.getProperty("line.separator"));
        boolean register = false;
        boolean gateway = false;
        boolean mgr = false;
        boolean service = false;
        for (String line : lines) {
            if (line.contains("gateway-0.5.0.jar")) {
                gateway = true;
            } else if (line.contains("keeper-mgr-0.5.0.jar")) {
                mgr = true;
            } else if (line.contains("keeper-service-0.5.0.jar")) {
                service = true;
            } else if (line.contains("register-server-0.5.0.jar")) {
                register = true;
            }
        }
        if (register && gateway && mgr && service) {
            for (int i = 0; i < 10; i++){
                TimeUnit.MILLISECONDS.sleep(6000);
                System.out.println("启动中,请耐心等待...");
            }
            System.out.println("启动成功！！");
        } else {
            if (!register) {
                System.out.println("register-server启动失败");
            }
            if (!gateway) {
                System.out.println("gateway启动失败");
            }
            if (!mgr) {
                System.out.println("keeper-mgr启动失败");
            }
            if (!service) {
                System.out.println("keeper-service启动失败");
            }
        }
    }

    private static boolean checkInit() {
        FileInputStream in = null;
        ZkService zk = null;
        try {
            System.out.println("加载config文件...");
            Properties pro = new Properties();
            in = new FileInputStream("config.properties");
            pro.load(in);
            zk = new ZkService(pro.getProperty("zk.str"), 5000);
            if (!zk.isExists("/DBusInit") && !zk.isExists("/DBus")) {
                System.out.println("DbusKeeper还未初始化,请先执行init.sh脚本");
                return false;
            }
            System.out.println("DbusKeeper已经初始化");
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("DbusKeeper初始化检测异常");
            return false;
        } finally {
            try {
                if (in != null) in.close();
                if (zk != null) zk.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private static String executeNormalCmd(String cmd) throws Exception {
        Process ps = Runtime.getRuntime().exec(cmd);
        ps.waitFor();
        return new BufferedReader(new InputStreamReader(ps.getInputStream()))
                .lines().parallel().collect(Collectors.joining(System.getProperty("line.separator")));
    }
}
