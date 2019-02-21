package com.creditease.dbus.canal.deploy.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;

import static com.creditease.dbus.canal.deploy.utils.FileUtils.writeAndPrint;

/**
 * User: 王少楠
 * Date: 2018-08-10
 * Desc:
 */
public class CanalUtils {

    public static void start(String canalPath,BufferedWriter bw) throws Exception{

        writeAndPrint(bw,"starting canal.....");

        try {
            String startPath = canalPath+"/bin/"+"startup.sh";
            String stopPath = canalPath+"/bin/"+"stop.sh";
            String cmd = "sh "+stopPath;
            writeAndPrint(bw,"exec: "+cmd);

            //停止已存在
            exec(cmd);
            cmd = "sh "+startPath;
            writeAndPrint(bw,"exec: "+cmd);

            exec(cmd);

        }catch (Exception e){
            writeAndPrint(bw,"************************************* START CANAL FAIL ************************************** ");

            throw  e;
        }
    }

    public static void stop(String canalPath,BufferedWriter bw) throws Exception{

        writeAndPrint(bw,"stopping canal.....");

        try {
            String stopPath = canalPath+"/bin/"+"stop.sh";
            String cmd = "sh "+stopPath;
            writeAndPrint(bw,"exec: "+cmd);

            //停止已存在
            exec(cmd);
        }catch (Exception e){
            writeAndPrint(bw,"************************************* STOP CANAL FAIL ***************************************");

            throw  e;
        }
    }

    public static void copyLogfiles(String canalPath,String dsName,BufferedWriter bw){
        try {
            //copy log file
            String cmd = "rm -f canal.log";
            writeAndPrint(bw,"exec: " + cmd);

            exec(cmd);

            cmd = "ln -s " + canalPath+"/logs/canal/canal.log canal.log";
            writeAndPrint(bw,"exec: " + cmd);

            exec(cmd);

            cmd = "rm -f "+dsName+".log";
            writeAndPrint(bw,"exec: " + cmd);

            exec(cmd);

            cmd = "ln -s " + canalPath+"/logs/"+dsName+"/"+dsName+".log "+ dsName+".log";
            writeAndPrint(bw,"exec: " + cmd);

            exec(cmd);
        }catch (Exception e){

        }
    }

    public static String exec(Object cmd) throws Exception{
        Process process = null;
        BufferedReader bufrIn = null;
        BufferedReader bufrError = null;
        StringBuilder result = new StringBuilder();

        try {
            if(cmd instanceof String) {
                process = Runtime.getRuntime().exec(cmd.toString());
            }else {
                String[] cmd2 = (String[]) cmd;
                process = Runtime.getRuntime().exec(cmd2);
            }

            int exitValue = process.waitFor();

            if (0 != exitValue) {
                bufrError = new BufferedReader(new InputStreamReader(process.getErrorStream(), "UTF-8"));
                String line = null;
                while ((line = bufrError.readLine()) != null) {
                    result.append(line).append("\n");
                }
                bufrError.close();
                throw new RuntimeException("");
            }else {
                // 读取输出
                // 获取命令执行结果, 有两个结果: 正常的输出 和 错误的输出（PS: 子进程的输出就是主进程的输入）
                bufrIn = new BufferedReader(new InputStreamReader(process.getInputStream(), "UTF-8"));
                String line = null;
                while ((line = bufrIn.readLine()) != null) {
                    result.append(line);
                }
                return result.toString();
            }
        }finally {
            if(bufrIn !=null){
                bufrIn.close();
            }
            if(process != null){
                process.destroy();
            }
        }

    }
}
