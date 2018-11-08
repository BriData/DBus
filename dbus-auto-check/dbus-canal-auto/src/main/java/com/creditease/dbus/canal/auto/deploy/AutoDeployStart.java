package com.creditease.dbus.canal.auto.deploy;
import com.creditease.dbus.canal.auto.deploy.container.CuratorContainer;
import com.creditease.dbus.canal.auto.deploy.bean.DeployPropsBean;
import com.creditease.dbus.canal.auto.deploy.utils.CanalUtils;
import com.creditease.dbus.canal.auto.deploy.utils.DBUtils;
import com.creditease.dbus.canal.auto.deploy.utils.FileUtils;
import com.creditease.dbus.canal.auto.deploy.utils.ZKUtils;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import static com.creditease.dbus.canal.auto.deploy.utils.FileUtils.*;

public class AutoDeployStart {

    private static final String DEPLOY_PROS_NAME="canal-auto.properties";

    public static void main(String[] args) throws Exception {

        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        BufferedWriter bw = null;

        try {
            //获得当前目录
            String currentPath =System.getProperty("user.dir");
            DeployPropsBean deployProps = FileUtils.readProps(currentPath+"/conf/"+DEPLOY_PROS_NAME,bw);
            //String canalPath =currentPath+"/"+"canal";
            String dsName = deployProps.getDsName();
            //创建report目录
            File reportDir = new File(currentPath,"reports");
            if(!reportDir.exists()) reportDir.mkdirs();

            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
            String strTime = sdf.format(new Date());
            File reportFile = new File(reportDir, "canal_deploy_"+dsName+"_"+ strTime + ".txt");

            fos = new FileOutputStream(reportFile);
            osw = new OutputStreamWriter(fos);
            bw = new BufferedWriter(osw);

//            DeployPropsBean deployProps = FileUtils.readProps(currentPath+"/conf/"+DEPLOY_PROS_NAME,bw);
//            String canalPath =currentPath+"/"+"canal";

            bw.write("************ CANAL DEPLOY BEGIN! ************");
            bw.newLine();
            printDeployProps(deployProps,bw);

            // 1.检测canal账号可用性（与源端db的连通性）
            DBUtils.checkDBAccount(deployProps,bw);

            //2.检查并创建canal节点
            ZKUtils.checkZKNode(deployProps,bw);

            /* 验证通过，开始部署和启动*/

            //2.5  将canal复制一份到当前目录下，并重命名作为部署目录
            String destPath = "canal-"+dsName;
            String cpCanalCmd = "cp -r canal "+destPath;
            bw.write("cp canal files:  "+cpCanalCmd);
            bw.newLine();
            CanalUtils.exec(cpCanalCmd);

            //canal目录
            String canalPath =currentPath+"/"+destPath;

            //3.修改canal.properties文件
            String canalProperties = "canal.properties";
            bw.write("------------ update canal.properties begin ------------ ");
            bw.newLine();
            WriteProperties(canalPath+"/conf/"+canalProperties,
                    "canal.port",String.valueOf(getAvailablePort()),bw);
            WriteProperties(canalPath+"/conf/"+canalProperties,
                    "canal.zkServers", deployProps.getZkPath()+"/DBus/Canal/canal-"+deployProps.getDsName(),bw);
            bw.write("------------ update canal.properties end ------------ ");
            bw.newLine();

            //4.创建canal目录下dsName文件夹
            checkExist(canalPath,deployProps.getDsName(),bw);
            //5.instance文件修改
            String instancePropsPath = canalPath+"/conf/"+deployProps.getDsName()+"/"+"instance.properties";
            bw.write("------------ update instance.properties begin ------------ ");
            bw.newLine();
            bw.write("instance file path "+instancePropsPath);
            bw.newLine();
            WriteProperties(instancePropsPath, "canal.instance.master.address",deployProps.getSlavePath(),bw);
            WriteProperties(instancePropsPath, "canal.instance.dbUsername",deployProps.getCanalUser(),bw);
            WriteProperties(instancePropsPath, "canal.instance.dbPassword",deployProps.getCanalPwd(),bw);
            WriteProperties(instancePropsPath, "canal.instance.connectionCharset"," UTF-8",bw);
            bw.write("------------ update canal.properties end ------------ ");
            bw.newLine();
            //创建canal节点
            //ZKUtils.checkZKNode(deployProps,bw);

            //启动canal
            CanalUtils.start(canalPath,bw);
            CanalUtils.copyLogfiles(canalPath,deployProps.getDsName(),bw);

            //无论怎么canal都会启动起来，如果出错，可以看日志;成功的话，echo pid
            String cmd = "ps aux | grep \""+canalPath+"/bin\" | grep -v \"grep\" | awk '{print $2}'";
            bw.write("exec: " + cmd);
            bw.newLine();
            try {
                String[] shell = {
                        "/bin/sh",
                        "-c",
                        cmd
                };

                String pid = CanalUtils.exec(shell);
                bw.write("canal 进程启动成功， pid " + pid);
                bw.newLine();
                bw.write("请手动检查当前目录下canal.log，和"+deployProps.getDsName()+".log中有无错误信息。");
                bw.newLine();
            }catch (Exception e){
                bw.write("exec fail.");
                bw.newLine();
                //如果执行失败,将canal进程停掉
                String stopPath = currentPath+"/canal/bin/"+"stop.sh";
                String stopCmd = "sh "+stopPath;
                CanalUtils.exec(stopCmd);
                throw e;
            }

            bw.write("************ CANAL DEPLOY SCCESS! ************");
            bw.newLine();


        }catch (Exception e){
            bw.write("************ CANAL DEPLOY ERROR! ************");
            bw.newLine();
        }finally {
            if(bw!=null){
                bw.flush();
                bw.close();
            }
            if(osw != null){
                osw.close();
            }
            if(fos != null){
                fos.close();
            }
            if(CuratorContainer.getInstance().getCurator() != null){
                CuratorContainer.getInstance().close();
            }
        }
    }


    private static void checkExist(String currentPath,String dsName,BufferedWriter bw) throws Exception{
        File instanceDirectory = new File(currentPath+"/conf/"+dsName);
        if(!instanceDirectory.exists()){
            //canal/conf/example
            File exampleDirectory = new File(currentPath+"/conf/"+"example");
            String cmd = MessageFormat.format("cp -r {0} {1}",exampleDirectory,instanceDirectory);
            Process process = Runtime.getRuntime().exec(cmd);
            int exitValue = process.waitFor();
            if (0 != exitValue) {
                bw.write("cp instance.properties error. from: "+exampleDirectory+" to "
                        +instanceDirectory);
                bw.newLine();
                throw new RuntimeException("cp instance.properties error,from: "+exampleDirectory+" to "
                        +instanceDirectory);
            }

        }

    }

    private static int getAvailablePort(){
        int startPort=10000;
        int endPort=40000;
        for(int port = startPort; port<= endPort;port++){
            if(isPortAvailable(port)){
                return port;
            }
        }
        System.out.println("canal端口自动分配失败");
        return -1;
    }
    private static boolean isPortAvailable(int port){
        try {
            bindPort("0.0.0.0", port);
            bindPort(InetAddress.getLocalHost().getHostAddress(), port);

            return true;
        }catch (IOException e){
            return false;
        }

    }
    private static void bindPort(String host,int port) throws IOException {
        Socket s = new Socket();
        s.bind(new InetSocketAddress(host,port));
        s.close();
    }

}
