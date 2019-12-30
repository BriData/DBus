/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2019 Bridata
 * ==
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * >>
 */


package com.creditease.dbus.ogg.handler.impl;

import com.creditease.dbus.ogg.bean.ExtractConfigBean;
import com.creditease.dbus.ogg.container.ExtractConfigContainer;
import com.creditease.dbus.ogg.handler.AbstractHandler;
import com.creditease.dbus.ogg.utils.FileUtil;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

import static com.creditease.dbus.ogg.utils.FileUtil.writeAndPrint;

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
        writeAndPrint("********************************** EXTRACT DEPLOY START *************************************");
        writeAndPrint("extract 配置文件: " + extractName + ".prm");

        try {
            if (extractConfig.getAppendTables() == null) {
                doDeployNewParamFile(extractConfig);
            } else {
                doDeployAppendParamFile(extractConfig);
            }
            String file = extractConfig.getOggHome() + "/dirprm/" + extractConfig.getExtrName() + ".prm";
            FileUtil.readFile(file, bw);
            writeAndPrint("********************************* EXTRACT DEPLOY SUCCESS ************************************");
        } catch (Exception e) {
            writeAndPrint("*********************************** EXTRACT DEPLOY FAIL *************************************");
            throw e;
        }
    }

    private void doDeployNewParamFile(ExtractConfigBean extractConfig) throws Exception {
        String extractName = extractConfig.getExtrName();
        BufferedWriter prmWriter = null;
        try {
            File paramFile = new File(extractConfig.getOggHome() + "/dirprm", extractName + ".prm");
            //如果存在的话，不覆盖，值添加table
            if (paramFile.exists()) {
                extractConfig.setAppendTables(extractConfig.getTables());
                doDeployAppendParamFile(extractConfig);
            } else {
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
                prmWriter.write("rmttrail " + extractConfig.getExtractFile());
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
