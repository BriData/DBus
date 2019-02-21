package com.creditease.dbus.canal.auto;

import com.creditease.dbus.canal.bean.DeployPropsBean;
import com.creditease.dbus.canal.utils.CanalUtils;
import com.creditease.dbus.canal.utils.DBUtils;
import com.creditease.dbus.canal.utils.FileUtils;
import com.creditease.dbus.canal.utils.ZKUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

import static com.creditease.dbus.canal.utils.FileUtils.writeAndPrint;

/**
 * User: 王少楠
 * Date: 2018-08-06
 * Desc: 检测配置的zookeeper和数据库地址是否正确,是否有权限连接
 * 1.检测canal账号可用性（与源端db的连通性）
 * 2.检查zk连通性,并创建canal节点
 * 3.根目录创建canal.log和instance.log的软连接,方便日志查看
 */
public class AutoCheckStart {

    private static final String DEPLOY_PROS_NAME = "canal-auto.properties";

    public static void main(String[] args) throws Exception {

        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        BufferedWriter bw = null;

        try {
            String currentPath = System.getProperty("user.dir");

            DeployPropsBean deployProps = FileUtils.readProps(currentPath + "/conf/" + DEPLOY_PROS_NAME);
            String dsName = deployProps.getDsName();

            File outdir = new File(currentPath, "reports");
            if (!outdir.exists()) outdir.mkdirs();
            //init report file
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
            String strTime = sdf.format(new Date());
            File reportFile = new File(outdir, "canal_check_" + dsName + "_" + strTime + ".txt");
            fos = new FileOutputStream(reportFile);
            osw = new OutputStreamWriter(fos);
            bw = new BufferedWriter(osw);
            FileUtils.init(bw);

            writeAndPrint( "************************************* CANAL CHECK BEGIN! ************************************");

            //1.检测canal账号可用性（与源端db的连通性）
            DBUtils.checkDBAccount(deployProps);
            //2.检查zk连通性,并创建canal节点
            ZKUtils.checkZKNode(deployProps);
            //3.根目录创建canal.log和instance.log的软连接,方便日志查看
            CanalUtils.copyLogfiles(currentPath, deployProps.getDsName());

            writeAndPrint( "**************************************** CANAL CHECK SUCCESS ********************************");
        } catch (Exception e) {
            e.printStackTrace();
            writeAndPrint( "***************************************** CANAL CHECK FAIL! *********************************");
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

}
