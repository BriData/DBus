package com.creditease.dbus.ogg.auto;

import com.creditease.dbus.ogg.handler.IHandler;
import com.creditease.dbus.ogg.handler.impl.*;
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
 * Date: 2018-08-24
 * Desc:ogg目标端
 */
public class Start {
    public static void main(String[] args) throws Exception {
        List<IHandler> actionEvents = new ArrayList<>();
        actionEvents.add(0, new LoadConfigFileHandler());
        actionEvents.add(new CheckKafkaHandler());
        actionEvents.add(new CheckDBHandler());
        actionEvents.add(new DeployPropsFileHandler());
        actionEvents.add(new DeployReplicateRaramHandler());

        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        BufferedWriter bw = null;
        try {
            File userDir = new File(System.getProperty("user.dir"));
            File outDir = new File(userDir, "reports");
            if (!outDir.exists()) outDir.mkdirs();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
            String strTime = sdf.format(new Date());
            File file = new File(outDir, "replicate_deploy_report_" + strTime + ".txt");

            fos = new FileOutputStream(file);
            osw = new OutputStreamWriter(fos);
            bw = new BufferedWriter(osw);
            FileUtil.init(bw);

            for (IHandler handler : actionEvents) {
                handler.process(bw);

            }
            writeAndPrint("************ success ************");

            System.out.println("************ success ************");

        } catch (Exception e) {
            writeAndPrint("************ fail! ************");

            System.out.println("************ fail! ************");
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
