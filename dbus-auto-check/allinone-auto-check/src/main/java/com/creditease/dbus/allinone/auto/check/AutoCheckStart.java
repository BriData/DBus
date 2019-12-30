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


package com.creditease.dbus.allinone.auto.check;

import com.creditease.dbus.allinone.auto.check.handler.IHandler;
import com.creditease.dbus.allinone.auto.check.handler.impl.*;
import com.creditease.dbus.commons.Constants;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SystemUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by Administrator on 2018/8/1.
 */
public class AutoCheckStart {

    static {
        System.setProperty("logs.home", SystemUtils.USER_DIR.replaceAll("\\\\", "/") + "/logs/");
        System.setProperty(Constants.SYS_PROPS_LOG4J_CONFIG, "file:" + SystemUtils.USER_DIR.replaceAll("\\\\", "/") + "/conf/log4j.xml");
    }

    public static void main(String[] args) {

        List<IHandler> list = new ArrayList<>();
        list.add(new LoadAutoCheckFileConfigHandler());
        list.add(new CheckDbHandler());
        list.add(new CheckBaseComponentsHandler());
        list.add(new CheckCanalHandler());
        list.add(new CheckTopologyHandler());
        list.add(new CheckFlowLineHandler());

        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        BufferedWriter bw = null;
        try {
            File userDir = new File(SystemUtils.USER_DIR.replaceAll("\\\\", "/"));
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
            String strTime = sdf.format(new Date());
            File outDir = new File(userDir, "reports/" + strTime);
            if (!outDir.exists()) outDir.mkdirs();
            File file = new File(outDir, "check_report.txt");

            fos = new FileOutputStream(file);
            osw = new OutputStreamWriter(fos);
            bw = new BufferedWriter(osw);
            for (IHandler handler : list) {
                handler.process(bw);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            IOUtils.closeQuietly(bw);
            IOUtils.closeQuietly(osw);
            IOUtils.closeQuietly(fos);
        }

    }

}
