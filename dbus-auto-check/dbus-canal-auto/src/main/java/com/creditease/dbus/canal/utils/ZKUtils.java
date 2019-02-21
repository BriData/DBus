package com.creditease.dbus.canal.utils;

import com.creditease.dbus.canal.bean.DeployPropsBean;
import com.creditease.dbus.commons.ZkService;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import static com.creditease.dbus.canal.utils.FileUtils.writeAndPrint;

/**
 * User: 王少楠
 * Date: 2018-08-09
 * Desc:
 */
public class ZKUtils {

    public static void checkZKNode(DeployPropsBean deployProps) throws Exception {
        ZkService zkService = null;
        try {
            writeAndPrint("********************************** CHECK CANAL ZK NODE BEGIN ********************************");

            String zkPath = deployProps.getZkPath();

            writeAndPrint("zk str:  " + zkPath);

            zkPath.replaceAll("，", ",");
            zkPath.replaceAll("：", ":");

            if (checkStr(zkPath) != 0) {
                writeAndPrint("connect zk fail ,check your zk config ");

                throw new RuntimeException();
            }

            zkService = new ZkService(zkPath, 5000);

            String canalPath = "/DBus/Canal/canal-" + deployProps.getDsName();
            if (!zkService.isExists("/DBus")) {
                zkService.createNode(canalPath, null);
                writeAndPrint("create zk node:  /DBus");

            }
            writeAndPrint("node exit ,skip zk node:  /DBus");

            if (!zkService.isExists("/DBus/Canal")) {
                zkService.createNode(canalPath, null);
                writeAndPrint("create zk node:  /DBus/Canal");

            }
            writeAndPrint("node exit ,skip zk node:  /DBus/Canal");

            if (!zkService.isExists(canalPath)) {
                zkService.createNode(canalPath, null);
                writeAndPrint("create zk node:  " + canalPath);

            }
            writeAndPrint("node exit ,skip zk node:  " + canalPath);

            writeAndPrint("******************************** CHECK CANAL ZK NODE SUCCESS ********************************");


        } catch (Exception e) {
            writeAndPrint("zookeeper连接失败，请检查zookeeper地址配置,格式 ip1:port1,ip2:port2 ！");

            writeAndPrint("********************************* CHECK CANAL ZK NODE FAIL **********************************");

            throw e;
        } finally {
            if (zkService != null) {
                zkService.close();
            }
        }
    }

    public static int checkStr(String zkPath) {
        int result = 0;
        String[] zkStrs = zkPath.split(",");
        for (String zkStr : zkStrs) {
            String[] info = zkStr.split(":");
            if (info.length != 2) {
                result = 1;
                break;
            }
            if (!checkConn(info[0], Integer.valueOf(info[1]))) {
                result = 1;
                break;
            }
        }
        return result;
    }

    private static boolean checkConn(String address, int port) {
        try {
            bindPort(address, port);
            return true;
        } catch (IOException e) {
            return false;
        }

    }

    private static void bindPort(String host, int port) throws IOException {
        Socket s = new Socket();
        s.connect(new InetSocketAddress(host, port));
        if (s != null) {
            s.close();
        }
    }
}
