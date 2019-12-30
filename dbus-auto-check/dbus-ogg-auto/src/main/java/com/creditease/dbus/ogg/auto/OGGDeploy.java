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


package com.creditease.dbus.ogg.auto;

import com.creditease.dbus.ogg.handler.IHandler;
import com.creditease.dbus.ogg.handler.impl.DeployExtractParamHandler;
import com.creditease.dbus.ogg.handler.impl.LoadExtractConfigHandler;
import com.creditease.dbus.ogg.utils.FileUtil;
import org.apache.commons.io.IOUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.creditease.dbus.ogg.utils.FileUtil.writeAndPrint;

/**
 * User: 王少楠
 * Date: 2018-08-29
 * Desc:ogg源端
 */
public class OGGDeploy {
    public static void main(String[] args) throws Exception {
        List<IHandler> handlers = new ArrayList<>();
        handlers.add(new LoadExtractConfigHandler());
        handlers.add(new DeployExtractParamHandler());

        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        BufferedWriter bw = null;
        try {
            File userDir = new File(System.getProperty("user.dir"));
            File outDir = new File(userDir, "reports");
            if (!outDir.exists()) outDir.mkdirs();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
            String strTime = sdf.format(new Date());
            File file = new File(outDir, "ogg_deploy_report_" + strTime + ".txt");

            fos = new FileOutputStream(file);
            osw = new OutputStreamWriter(fos);
            bw = new BufferedWriter(osw);
            FileUtil.init(bw);

            writeAndPrint("************************************** OGG DEPLOY BEGIN  ************************************");

            for (IHandler handler : handlers) {
                handler.process(bw);
            }
            writeAndPrint("************************************ OGG DEPLOY SUCCESS *************************************");


        } catch (Exception e) {
            e.printStackTrace();
            writeAndPrint("************************************* OGG DEPLOY FAIL ***************************************");

        } finally {
            if (bw != null) {
                bw.flush();
            }
            IOUtils.closeQuietly(bw);
            IOUtils.closeQuietly(osw);
            IOUtils.closeQuietly(fos);
        }
    }
}
