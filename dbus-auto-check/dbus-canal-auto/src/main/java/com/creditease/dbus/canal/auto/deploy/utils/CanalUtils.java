package com.creditease.dbus.canal.auto.deploy.utils;

import java.io.BufferedWriter;

/**
 * User: 王少楠
 * Date: 2018-08-10
 * Desc:
 */
public class CanalUtils {

    public static void start(String currentPath,BufferedWriter bw) throws Exception{

        bw.write("------------ starting canal.....");
        bw.newLine();
        try {
            String startPath = currentPath+"/canal/bin/"+"startup.sh";
            String stopPath = currentPath+"/canal/bin/"+"stop.sh";
            String cmd = "sh "+stopPath;
            bw.write("exec: "+cmd);
            bw.newLine();
            //停止已存在
            exec(cmd);
            cmd = "sh "+startPath;
            bw.write("exec: "+cmd);
            bw.newLine();
            exec(cmd);

        }catch (Exception e){
            bw.write("------------ start canal fail ------------");
            bw.newLine();
            throw  e;
        }
    }

    public static void copyLogfiles(String currentPath,String dsName,BufferedWriter bw){
        try {
            //copy log file
            String cmd = "rm -f canal.log";
            bw.write("exec: " + cmd);
            bw.newLine();
            exec(cmd);

            cmd = "ln -s " + currentPath + "/canal/logs/canal/canal.log canal.log";
            bw.write("exec: " + cmd);
            bw.newLine();
            exec(cmd);

            cmd = "rm -f "+dsName+".log";
            bw.write("exec: " + cmd);
            bw.newLine();
            exec(cmd);

            cmd = "ln -s " + currentPath + "/canal/logs/"+dsName+"/"+dsName+".log "+ dsName+".log";
            bw.write("exec: " + cmd);
            bw.newLine();
            exec(cmd);
        }catch (Exception e){

        }
    }

    private static void exec(String cmd) throws Exception{
        Process process = Runtime.getRuntime().exec(cmd);
        int exitValue = process.waitFor();
        if (0 != exitValue) {
            throw new RuntimeException("");
        }
    }
}
