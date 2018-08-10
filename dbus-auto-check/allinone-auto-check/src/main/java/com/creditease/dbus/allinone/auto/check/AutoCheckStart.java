package com.creditease.dbus.allinone.auto.check;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.creditease.dbus.allinone.auto.check.handler.IHandler;
import com.creditease.dbus.allinone.auto.check.handler.impl.CheckBaseComponentsHandler;
import com.creditease.dbus.allinone.auto.check.handler.impl.CheckCanalHandler;
import com.creditease.dbus.allinone.auto.check.handler.impl.CheckDbHandler;
import com.creditease.dbus.allinone.auto.check.handler.impl.CheckFlowLineHandler;
import com.creditease.dbus.allinone.auto.check.handler.impl.CheckTopologyHandler;
import com.creditease.dbus.allinone.auto.check.handler.impl.LoadAutoCheckFileConfigHandler;
import com.creditease.dbus.commons.Constants;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SystemUtils;

/**
 * Created by Administrator on 2018/8/1.
 */
public class AutoCheckStart {

    static {
        System.setProperty("logs.home", SystemUtils.USER_DIR.replaceAll("\\\\", "/") + "/logs/");
        System.setProperty(Constants.SYS_PROPS_LOG4J_CONFIG,"file:" + SystemUtils.USER_DIR.replaceAll("\\\\", "/") + "/conf/log4j.xml");
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

        } finally {
            IOUtils.closeQuietly(bw);
            IOUtils.closeQuietly(osw);
            IOUtils.closeQuietly(fos);
        }

    }

}
