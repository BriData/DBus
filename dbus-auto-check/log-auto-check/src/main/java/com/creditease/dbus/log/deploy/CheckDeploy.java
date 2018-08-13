package com.creditease.dbus.log.deploy;

import com.creditease.dbus.log.handler.IHandler;
import com.creditease.dbus.log.handler.impl.CheckKafkaHandler;
import com.creditease.dbus.log.handler.impl.DeployFileConfigHandler;
import com.creditease.dbus.log.handler.impl.LoadConfigFileHandler;
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

public class CheckDeploy {
    public static void main(String[] args) {

        List<IHandler> list = new ArrayList<>();
        list.add(new LoadConfigFileHandler());
        list.add(new CheckKafkaHandler());
        list.add(new DeployFileConfigHandler());

        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        BufferedWriter bw = null;
        try {
            File userDir = new File(SystemUtils.USER_DIR.replaceAll("\\\\", "/"));
            File outDir = new File(userDir, "reports");
            if (!outDir.exists()) outDir.mkdirs();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
            String strTime = sdf.format(new Date());
            File file = new File(outDir, "check_deploy_report_" + strTime + ".txt");

            fos = new FileOutputStream(file);
            osw = new OutputStreamWriter(fos);
            bw = new BufferedWriter(osw);
            for (IHandler handler : list) {
                handler.processCheckDeploy(bw);
            }
        } catch (Exception e) {

        } finally {
            IOUtils.closeQuietly(bw);
            IOUtils.closeQuietly(osw);
            IOUtils.closeQuietly(fos);
        }
    }
}