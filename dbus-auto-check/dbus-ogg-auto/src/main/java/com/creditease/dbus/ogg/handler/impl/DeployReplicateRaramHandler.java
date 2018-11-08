package com.creditease.dbus.ogg.handler.impl;

import com.creditease.dbus.ogg.bean.ConfigBean;
import com.creditease.dbus.ogg.container.AutoCheckConfigContainer;
import com.creditease.dbus.ogg.handler.AbstractHandler;
import com.creditease.dbus.ogg.utils.FileUtil;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

/**
 * User: 王少楠
 * Date: 2018-08-30
 * Desc:
 */
public class DeployReplicateRaramHandler extends AbstractHandler {

    public void checkDeploy(BufferedWriter bw) throws Exception {
        deployFile(bw);
    }

    private void deployFile(BufferedWriter bw) throws Exception{
        ConfigBean config = AutoCheckConfigContainer.getInstance().getConfig();
        String replicateFileName = config.getDsName()+".prm";
        System.out.println("============================================");
        System.out.println("部署extract 配置文件: "+replicateFileName);
        bw.write("============================================");
        bw.newLine();
        bw.write("部署extract 配置文件"+replicateFileName);
        bw.newLine();
        try {
            if (config.getAppendTables() == null) {
                doDeployNewParamFile(config);
            } else {
                doDeployAppendParamFile(config);
            }
            System.out.println("部署成功: ");
            bw.write("部署成功: ");
            bw.newLine();

            String replName = config.getDsName()+".prm";
            String fileName = config.getOggBigHome() + "/dirprm/"+replName;
            FileUtil.readFile(fileName,bw);
        } catch (Exception e) {
            System.out.println("部署失败！！！");
            bw.write("部署失败！！！");
            bw.newLine();
        } finally {

        }
    }

    private void doDeployAppendParamFile(ConfigBean config) throws Exception{
        String replName = config.getDsName();
        BufferedWriter prmWriter = null;
        try {
            File paramFile = new File(config.getOggBigHome() + "/dirprm", replName + ".prm");
            prmWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(paramFile, true)));
            //追加tables
            String[] tables = config.getAppendTables();
            for (String table : tables) {
                prmWriter.write("MAP " + table + ", ");
                prmWriter.write("TARGET " + table + ";");
                prmWriter.newLine();
            }

        } catch (Exception e) {
            throw e;
        } finally {
            if (prmWriter != null) {
                prmWriter.flush();
                prmWriter.close();
            }
        }
    }

    private void doDeployNewParamFile(ConfigBean config) throws Exception{
        String repliName = config.getDsName();
        BufferedWriter prmWriter = null;
        try {
            File paramFile = new File(config.getOggBigHome() + "/dirprm", repliName + ".prm");
            //如果存在的话，不覆盖，值添加table
            if(paramFile.exists()){
                config.setAppendTables(config.getTables());
                doDeployAppendParamFile(config);
            }else {
                paramFile.createNewFile();
                prmWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(paramFile)));
                prmWriter.write("REPLICAT " + repliName);
                prmWriter.newLine();
                prmWriter.write("SETENV (NLS_LANG=" + config.getNlsLang() + ")");
                prmWriter.newLine();
                //props的文件名称，与dsName一致
                prmWriter.write("TARGETDB LIBFILE libggjava.so SET property=dirprm/" + config.getDsName()+".props");
                prmWriter.newLine();
                prmWriter.write("DDL INCLUDE ALL ");
                prmWriter.newLine();
                prmWriter.newLine();
                prmWriter.write("GROUPTRANSOPS 500");
                prmWriter.newLine();
                prmWriter.write("MAXTRANSOPS 1000");
                prmWriter.newLine();
                prmWriter.newLine();
                //tables
                String[] tables = config.getTables();
                for (String table : tables) {
                    prmWriter.write("MAP " + table + ", ");
                    prmWriter.write("TARGET " + table + ";");
                    prmWriter.newLine();
                }
            }
        } catch (Exception e) {
            throw e;
        } finally {
            if (prmWriter != null) {
                prmWriter.flush();
                prmWriter.close();
            }
        }
        
    } 
}
