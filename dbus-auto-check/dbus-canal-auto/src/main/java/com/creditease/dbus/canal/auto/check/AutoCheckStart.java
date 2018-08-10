package com.creditease.dbus.canal.auto.check;

import com.creditease.dbus.canal.auto.deploy.container.CuratorContainer;
import com.creditease.dbus.canal.auto.check.utils.JDBCUtils;
import com.creditease.dbus.canal.auto.deploy.utils.ZKUtils;
import com.creditease.dbus.canal.auto.deploy.bean.DeployPropsBean;
import com.creditease.dbus.canal.auto.deploy.utils.FileUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * User: 王少楠
 * Date: 2018-08-06
 * Desc:
 */
public class AutoCheckStart {

    private static final String DEPLOY_PROS_NAME="canal-auto.properties";
    public static void main(String[] args)  throws Exception{

        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        BufferedWriter bw = null;

        try {
            String currentPath =System.getProperty("user.dir");
            //init report file
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
            String strTime = sdf.format(new Date());
            File reportFile = new File(currentPath, "canal_check_report" + strTime + ".txt");
            fos = new FileOutputStream(reportFile);
            osw = new OutputStreamWriter(fos);
            bw = new BufferedWriter(osw);


            DeployPropsBean deployProps = FileUtils.readProps(currentPath+"/conf/"+DEPLOY_PROS_NAME,bw);
            String basePath =deployProps.getCanalInstallPath();



            bw.write("************ CANAL CHECK BEGIN! ************");
            bw.newLine();

            // 1.检测canal账号可用性（与源端db的连通性）
            checkDBAccount(deployProps,bw);
            // 2.检测zk的连通性， 3.检测脚本需自动检测zk上配套需要的canal节点是否存在；
            ZKUtils.checkZKNode(deployProps,bw);
            // 4.extractor连上的offset信息等，生成检测报告，
            // 5.并将canal日志、canal-mydb（例）日志拷贝到检测报告所在文件夹。
            cpLogs(basePath,currentPath,deployProps.getDsName(),bw);


            bw.write("************ CANAL CHECK SUCCESS ************");
            bw.newLine();

        } catch (Exception e) {
            bw.write("************ CANAL CHECK FAIL! ************");
        }finally {
            if(bw!=null){
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

    private static  void checkDBAccount(DeployPropsBean deployProps,BufferedWriter bw) throws Exception{
        try {
            bw.write("-----check database canal account  begin ");
            bw.newLine();
            bw.write("canal user: " + deployProps.getCanalUser());
            bw.newLine();
            bw.write("canal pwd: " + deployProps.getCanalPwd());
            bw.newLine();
            bw.write("slave url: " + deployProps.getSlavePath());
            bw.newLine();
            Connection conn = JDBCUtils.getConn(deployProps.getSlavePath(),
                    deployProps.getCanalUser(), deployProps.getCanalPwd(), bw);
            if (!conn.isClosed()) {
                bw.write("-----check database canal account success");
                bw.newLine();
                JDBCUtils.closeAll(conn, null, null, bw);
            } else {
                JDBCUtils.closeAll(conn, null, null, bw);
                throw new Exception();
            }
        }catch (Exception e){
            bw.write("-----check database canal account fail ------------");
            bw.newLine();
            throw e;
        }
    }

    private static void cpLogs(String basePath,String currentPath,String dsName,BufferedWriter bw) throws Exception{
        try {
            bw.write("-----copy log-files begin.");
            bw.newLine();
            String canalLogFile = basePath + "/logs/canal/canal.log";
            String dsLogFile =basePath + "/logs/" + dsName + "/" + dsName + ".log";
            bw.write("cp canal.log file to current directory...");
            bw.newLine();
            FileUtils.copyFile(canalLogFile,currentPath+"/"+"canal.log",bw);
            bw.write("cp "+dsName+".log file to current directory...");
            bw.newLine();
            FileUtils.copyFile(dsLogFile,currentPath+"/"+dsName+".log",bw);
            bw.write("-----copy log-files end.");
            bw.newLine();
        }catch (Exception e){
            bw.write("-----copy log-files error ");
            bw.newLine();
            throw e;
        }


    }

}
