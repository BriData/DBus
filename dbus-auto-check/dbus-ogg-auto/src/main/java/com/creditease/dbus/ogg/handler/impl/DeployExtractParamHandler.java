package com.creditease.dbus.ogg.handler.impl;

import com.creditease.dbus.ogg.bean.ExtractConfigBean;
import com.creditease.dbus.ogg.container.ExtractConfigContainer;
import com.creditease.dbus.ogg.handler.AbstractHandler;
import com.creditease.dbus.ogg.utils.FileUtil;

import java.io.*;

/**
 * User: 王少楠
 * Date: 2018-08-28
 * Desc:
 */
public class DeployExtractParamHandler extends AbstractHandler {

    public void checkDeploy(BufferedWriter bw) throws Exception {
        deployExtractParam(bw);
    }

    private void deployExtractParam(BufferedWriter bw) throws Exception {
        ExtractConfigBean extractConfig = ExtractConfigContainer.getInstance().getExtrConfig();
        String extractName = extractConfig.getExtrName();
        System.out.println("============================================");
        System.out.println("开始部署extract 配置文件: " + extractName + ".prm");
        bw.write("============================================");
        bw.newLine();
        bw.write("开始部署extract 配置文件: " + extractName + ".prm");
        bw.newLine();
        try {
            if (extractConfig.getAppendTables() == null) {
                doDeployNewParamFile(extractConfig);
            } else {
                doDeployAppendParamFile(extractConfig);
            }
            System.out.println("部署成功:");
            bw.write("部署成功:");
            bw.newLine();

            //部署成功，在前台打印出结果
            String file = extractConfig.getOggHome() + "/dirprm/"+extractConfig.getExtrName() + ".prm";
            FileUtil.readFile(file,bw);
        } catch (Exception e) {
            System.out.println("部署失败！！！");
            bw.write("部署失败！！！");
            bw.newLine();
        } finally {

        }
    }

    private void doDeployNewParamFile(ExtractConfigBean extractConfig) throws Exception {
        String extractName = extractConfig.getExtrName();
        BufferedWriter prmWriter = null;
        try {
            File paramFile = new File(extractConfig.getOggHome() + "/dirprm", extractName + ".prm");
            //如果存在的话，不覆盖，值添加table
            if(paramFile.exists()){
                extractConfig.setAppendTables(extractConfig.getTables());
                doDeployAppendParamFile(extractConfig);
            }else {
                paramFile.createNewFile();
                prmWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(paramFile)));
                prmWriter.write("EXTRACT " + extractName.toUpperCase());
                prmWriter.newLine();
                prmWriter.write("SETENV (NLS_LANG=" + extractConfig.getNlsLang() + ")");
                prmWriter.newLine();
                prmWriter.write("USERID " + extractConfig.getOggUser() + ",PASSWORD " + extractConfig.getOggPwd());
                prmWriter.newLine();
                prmWriter.write("RMTHOST " + extractConfig.getRmHost() + ", MGRPORT " + extractConfig.getMgrPort());
                prmWriter.newLine();
                prmWriter.write("rmttrail " +  extractConfig.getExtractFile());
                prmWriter.newLine();
                prmWriter.write("DDL INCLUDE MAPPED");
                prmWriter.newLine();
                prmWriter.write("TRANLOGOPTIONS DBLOGREADER");
                prmWriter.newLine();
                prmWriter.newLine();
                //tables
                String[] tables = extractConfig.getTables();
                for (String table : tables) {
                    prmWriter.write("TABLE " + table + ";");
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

    private void doDeployAppendParamFile(ExtractConfigBean extractConfig) throws Exception {
        String extractName = extractConfig.getExtrName();
        BufferedWriter prmWriter = null;
        try {
            File paramFile = new File(extractConfig.getOggHome() + "/dirprm", extractName + ".prm");
            prmWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(paramFile, true)));
            //追加tables
            String[] tables = extractConfig.getAppendTables();
            for (String table : tables) {
                prmWriter.write("TABLE " + table + ";");
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


}
