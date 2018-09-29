package com.creditease.dbus.canal.auto.check;

import com.creditease.dbus.canal.auto.deploy.bean.DeployPropsBean;
import com.creditease.dbus.canal.auto.deploy.container.CuratorContainer;
import com.creditease.dbus.canal.auto.deploy.utils.CanalUtils;
import com.creditease.dbus.canal.auto.deploy.utils.DBUtils;
import com.creditease.dbus.canal.auto.deploy.utils.FileUtils;
import com.creditease.dbus.canal.auto.deploy.utils.ZKUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
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


            DeployPropsBean deployProps = FileUtils.readProps(currentPath+"/conf/"+DEPLOY_PROS_NAME,bw);
            String dsName = deployProps.getDsName();

            String basePath = currentPath+"/"+"canal";
            File outdir = new File(currentPath,"reports");
            if(!outdir.exists()) outdir.mkdirs();
            //init report file
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
            String strTime = sdf.format(new Date());
            File reportFile = new File(outdir, "canal_check_"+dsName+"_"+ strTime + ".txt");
            fos = new FileOutputStream(reportFile);
            osw = new OutputStreamWriter(fos);
            bw = new BufferedWriter(osw);



            bw.write("************ CANAL CHECK BEGIN! ************");
            bw.newLine();

            // 1.检测canal账号可用性（与源端db的连通性）
            DBUtils.checkDBAccount(deployProps,bw);
            // 2.检测zk的连通性， 3.检测脚本需自动检测zk上配套需要的canal节点是否存在；
            ZKUtils.checkZKNode(deployProps,bw);
            // 4.extractor连上的offset信息等，生成检测报告，
            // 5.并将canal日志、canal-mydb（例）日志拷贝到检测报告所在文件夹。
            //cpLogs(basePath,currentPath,deployProps.getDsName(),bw);
            CanalUtils.copyLogfiles(currentPath,deployProps.getDsName(),bw);


            bw.write("************ CANAL CHECK SUCCESS ************");
            bw.newLine();

        } catch (Exception e) {
            bw.write("************ CANAL CHECK FAIL! ************");
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
