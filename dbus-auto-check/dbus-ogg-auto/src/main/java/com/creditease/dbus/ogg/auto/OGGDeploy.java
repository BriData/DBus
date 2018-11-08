package com.creditease.dbus.ogg.auto;

import com.creditease.dbus.ogg.handler.IHandler;
import com.creditease.dbus.ogg.handler.impl.DeployExtractParamHandler;
import com.creditease.dbus.ogg.handler.impl.LoadExtractConfigHandler;
import org.apache.commons.io.IOUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * User: 王少楠
 * Date: 2018-08-29
 * Desc:
 */
public class OGGDeploy {
    public static void main(String[] args) throws Exception{
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
            for (IHandler handler : handlers) {
                if(!handler.process(bw)){
                    throw new RuntimeException("fail.");
                }
            }
            bw.write("************ success ************");
            bw.newLine();
            System.out.println("************ success ************");

        } catch (Exception e) {
            bw.write("************ fail! ************");
            bw.newLine();
            System.out.println("************ fail! ************");
        } finally {
            if(bw != null) {
                bw.flush();
            }
            IOUtils.closeQuietly(bw);
            IOUtils.closeQuietly(osw);
            IOUtils.closeQuietly(fos);
        }
    }
}
