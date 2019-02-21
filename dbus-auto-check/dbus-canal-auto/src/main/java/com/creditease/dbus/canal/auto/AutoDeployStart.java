package com.creditease.dbus.canal.auto;

import com.creditease.dbus.canal.bean.DeployPropsBean;
import com.creditease.dbus.canal.utils.CanalUtils;
import com.creditease.dbus.canal.utils.DBUtils;
import com.creditease.dbus.canal.utils.FileUtils;
import com.creditease.dbus.canal.utils.ZKUtils;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static com.creditease.dbus.canal.utils.FileUtils.writeAndPrint;
import static com.creditease.dbus.canal.utils.FileUtils.writeProperties;

/**
 * User: 王少楠
 * Date: 2018-08-06
 * Desc: 自动部署并且启动canal
 * 1.检测canal账号可用性（与源端db的连通性）
 * 2.检查zk连通性,并创建canal节点
 * 3.将canal复制一份到当前目录下，并重命名作为部署目录
 * 4.根据用户配置修改配置文件
 * 5.启动canal
 * 6.根目录创建canal.log和instance.log的软连接,方便日志查看
 */
public class AutoDeployStart {

    private static final String DEPLOY_PROS_NAME = "canal-auto.properties";

    public static void main(String[] args) throws Exception {

        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        BufferedWriter bw = null;

        try {
            //获得当前目录
            String userdir = System.getProperty("user.dir");
            DeployPropsBean deployProps = FileUtils.readProps(userdir + "/conf/" + DEPLOY_PROS_NAME);

            String dsName = deployProps.getDsName();
            //创建report目录
            File reportDir = new File(userdir, "reports");
            if (!reportDir.exists()) {
                reportDir.mkdirs();
            }

            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
            String strTime = sdf.format(new Date());
            File reportFile = new File(reportDir, "canal_deploy_" + dsName + "_" + strTime + ".txt");

            fos = new FileOutputStream(reportFile);
            osw = new OutputStreamWriter(fos);
            bw = new BufferedWriter(osw);
            FileUtils.init(bw);

            writeAndPrint("************************************ CANAL CONFIG CHECK BEGIN!*******************************");
            writeAndPrint("数据源名称: " + deployProps.getDsName());
            writeAndPrint("zk地址: " + deployProps.getZkPath());
            writeAndPrint("备库地址: " + deployProps.getSlavePath());
            writeAndPrint("canal 用户名: " + deployProps.getCanalUser());
            writeAndPrint("canal 密码: " + deployProps.getCanalPwd());

            //1.检测canal账号可用性（与源端db的连通性）
            DBUtils.checkDBAccount(deployProps);

            //2.检查zk连通性,并创建canal节点
            ZKUtils.checkZKNode(deployProps);

            writeAndPrint("************************************ CANAL CONFIG CHECK SUCCESS!*****************************");
            writeAndPrint("**************************************** CANAL DEPLOY BEGIN!*********************************");

            String destPath = "canal-" + dsName;
            String canalPath = userdir + "/" + destPath;

            //3.将canal复制一份到当前目录下，并重命名作为部署目录
            cpCanalFiles(destPath, canalPath, dsName);

            //4.根据用户配置修改配置文件
            editCanalConfigFiles(deployProps, dsName, canalPath);

            //5.启动canal
            CanalUtils.start(canalPath);

            //6.根目录创建canal.log和instance.log的软连接,方便日志查看
            CanalUtils.copyLogfiles(canalPath, deployProps.getDsName());

            //无论怎么canal都会启动起来，如果出错，可以看日志;成功的话，echo pid
            String cmd = "ps aux | grep \"" + canalPath + "/bin\" | grep -v \"grep\" | awk '{print $2}'";
            writeAndPrint("exec: " + cmd);

            try {
                String[] shell = {
                        "/bin/sh",
                        "-c",
                        cmd
                };

                String pid = CanalUtils.exec(shell);
                writeAndPrint("canal 进程启动成功， pid " + pid);

                writeAndPrint("请手动检查当前目录下canal.log，和" + deployProps.getDsName() + ".log中有无错误信息。");

            } catch (Exception e) {
                writeAndPrint("exec fail.");

                //如果执行失败,将canal进程停掉
                String stopPath = userdir + "/canal/bin/" + "stop.sh";
                String stopCmd = "sh " + stopPath;
                CanalUtils.exec(stopCmd);
                throw e;
            }

            writeAndPrint("********************************* CANAL DEPLOY SCCESS! **************************************");


        } catch (Exception e) {
            writeAndPrint("*********************************** CANAL DEPLOY FAIL! **************************************");
            e.printStackTrace();
        } finally {
            if (bw != null) {
                bw.flush();
                bw.close();
            }
            if (osw != null) {
                osw.close();
            }
            if (fos != null) {
                fos.close();
            }
        }
    }

    private static void editCanalConfigFiles(DeployPropsBean deployProps, String dsName, String canalPath) throws Exception {
        //canal.properties文件编辑
        writeAndPrint("************************************ EDIT CANAL.PROPERTIES BEGIN ****************************");

        String canalProperties = "canal.properties";
        int canalPort = getAvailablePort();
        writeProperties(canalPath + "/conf/" + canalProperties, "canal.port", "canal.port = " + canalPort);
        //canal-1.1.1需要这个参数
        //ArrayList<Integer> ports = new ArrayList<>();
        //ports.add(canalPort);
        //int pullPort = getAvailablePort(ports);
        //writeProperties(canalPath + "/conf/" + canalProperties, "canal.metrics.pull.port", "canal.metrics.pull.port = " + pullPort);
        writeProperties(canalPath + "/conf/" + canalProperties, "canal.zkServers",
                "canal.zkServers = " + deployProps.getZkPath() + "/DBus/Canal/canal-" + deployProps.getDsName());
        writeProperties(canalPath + "/conf/" + canalProperties, "canal.destinations", "canal.destinations = " + dsName);
        writeProperties(canalPath + "/conf/" + canalProperties, "canal.auto.scan", "canal.auto.scan = false");
        writeProperties(canalPath + "/conf/" + canalProperties, "canal.instance.filter.query.dcl", "canal.instance.filter.query.dcl = true");
        writeProperties(canalPath + "/conf/" + canalProperties, "canal.instance.filter.query.dml", "canal.instance.filter.query.dml = true");
        writeProperties(canalPath + "/conf/" + canalProperties, "canal.instance.binlog.format", "canal.instance.binlog.format = ROW");
        writeProperties(canalPath + "/conf/" + canalProperties, "canal.instance.binlog.image", "canal.instance.binlog.image = FULL");
        writeProperties(canalPath + "/conf/" + canalProperties, "classpath:spring/file-instance.xml",
                "#canal.instance.global.spring.xml = classpath:spring/file-instance.xml");
        writeProperties(canalPath + "/conf/" + canalProperties, "classpath:spring/default-instance.xml",
                "canal.instance.global.spring.xml = classpath:spring/default-instance.xml");
        writeAndPrint("********************************** EDIT CANAL.PROPERTIES SUCCESS ****************************");


        //4.创建canal目录下dsName文件夹
        //checkExist(canalPath, deployProps.getDsName());
        //instance文件编辑
        String instancePropsPath = canalPath + "/conf/" + deployProps.getDsName() + "/" + "instance.properties";
        writeAndPrint("****************************** UPDATE INSTANCE.PROPERTIES BEGIN *****************************");

        writeAndPrint("instance file path " + instancePropsPath);

        writeProperties(instancePropsPath, "canal.instance.master.address", "canal.instance.master.address = " + deployProps.getSlavePath());
        writeProperties(instancePropsPath, "canal.instance.dbUsername", "canal.instance.dbUsername = " + deployProps.getCanalUser());
        writeProperties(instancePropsPath, "canal.instance.dbPassword", "canal.instance.dbPassword = " + deployProps.getCanalPwd());
        writeProperties(instancePropsPath, "canal.instance.connectionCharset", " canal.instance.connectionCharset = UTF-8");
        writeAndPrint("***************************** UPDATE INSTANCE.PROPERTIES SUCCESS ****************************");

    }

    private static void cpCanalFiles(String destPath, String canalPath, String dsName) throws Exception {
        writeAndPrint("**************************************** COPY CANAL BEGIN ***********************************");


        //如果canal处于启动中,先停止
        String pidPath = canalPath + "/bin/canal.pid";
        if (checkExist(pidPath)) {
            CanalUtils.stop(canalPath);
            String cpFiles = "cp -r canal/. " + destPath;
            writeAndPrint("copy canal files:  " + cpFiles);

            CanalUtils.exec(cpFiles);
        } else {
            String cpFiles = "cp -r canal/. " + destPath;
            writeAndPrint("copy canal files:  " + cpFiles);

            CanalUtils.exec(cpFiles);
        }

        String confPath = canalPath + "/conf";
        String cmd = "cp -r " + confPath + "/example/. " + confPath + "/" + dsName;
        writeAndPrint("copy instance files:  " + cmd);

        CanalUtils.exec(cmd);
        writeAndPrint("**************************************** COPY CANAL SUCCESS *********************************");

    }


    private static void checkExist(String userdir, String dsName) throws Exception {
        File instanceDirectory = new File(userdir + "/conf/" + dsName);
        if (!instanceDirectory.exists()) {
            //canal/conf/example
            File exampleDirectory = new File(userdir + "/conf/" + "example");
            String cmd = MessageFormat.format("cp -r {0} {1}", exampleDirectory, instanceDirectory);
            Process process = Runtime.getRuntime().exec(cmd);
            int exitValue = process.waitFor();
            if (0 != exitValue) {
                writeAndPrint("cp instance.properties error. from: " + exampleDirectory + " to "
                        + instanceDirectory);

                throw new RuntimeException("cp instance.properties error,from: " + exampleDirectory + " to "
                        + instanceDirectory);
            }
        }
    }

    private static boolean checkExist(String path) throws Exception {
        File file = new File(path);
        if (file.exists()) {
            return true;
        }
        return false;
    }

    private static int getAvailablePort() {
        int startPort = 10000;
        int endPort = 40000;
        for (int port = startPort; port <= endPort; port++) {
            if (isPortAvailable(port)) {
                return port;
            }
        }
        System.out.println("canal端口自动分配失败");
        return -1;
    }

    private static int getAvailablePort(List<Integer> ports) {
        int startPort = 10000;
        int endPort = 40000;
        for (int port = startPort; port <= endPort; port++) {
            if (isPortAvailable(port) && !ports.contains(port)) {
                return port;
            }
        }
        System.out.println("canal端口自动分配失败");
        return -1;
    }

    private static boolean isPortAvailable(int port) {
        try {
            bindPort("0.0.0.0", port);
            bindPort(InetAddress.getLocalHost().getHostAddress(), port);

            return true;
        } catch (IOException e) {
            return false;
        }

    }

    private static void bindPort(String host, int port) throws IOException {
        Socket s = new Socket();
        s.bind(new InetSocketAddress(host, port));
        s.close();
    }

}
