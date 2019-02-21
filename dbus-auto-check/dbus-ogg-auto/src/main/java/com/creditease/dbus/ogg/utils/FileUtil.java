package com.creditease.dbus.ogg.utils;

import java.io.*;
import java.util.*;

/**
 * User: 王少楠
 * Date: 2018-08-24
 * Desc:
 */
public class FileUtil {
    public static BufferedWriter bw = null;
    public static boolean inited = false;

    public static void init(BufferedWriter bufferedWriter) {
        bw = bufferedWriter;
        inited = true;
    }

    /**
     * 更新properties文件某key的value
     */
    public static void WriteProperties(String source, Properties props, String destination, BufferedWriter bw) throws Exception {
        BufferedReader br = null;
        PrintWriter pw = null;
        try {
            File ins = new File(source);
            br = new BufferedReader(new InputStreamReader(new FileInputStream(ins)));

            List<String> fileContent = new LinkedList();
            while (true) {
                //读取一个属性
                String line = br.readLine();
                if (line == null) {
                    break;
                }
                for (Map.Entry<Object, Object> entry : props.entrySet()) {
                    String key = (String) entry.getKey();
                    String value = (String) entry.getValue();
                    if (line.contains(key)) {
                        fileContent.add(key + "=" + value);
                        writeAndPrint("设置属性： " + key + "=" + value);
                    } else {
                        fileContent.add(line);
                    }
                }
            }

            File outs = new File(destination);
            if (!outs.exists()) {
                outs.createNewFile();
            }
            pw = new PrintWriter(outs);
            for (String line : fileContent) {
                pw.println(line);
            }
            pw.flush();
            writeAndPrint("更新配置文件成功: " + destination);
        } catch (Exception e) {
            throw e;
        } finally {
            try {
                close(br);
                close(pw);
            } catch (Exception e) {
            }
        }
    }

    /**
     * 更新properties文件某key的value
     */
    public static void WriteProperties(String source, HashMap<String, String> props, String destination) throws Exception {
        BufferedReader br = null;
        PrintWriter pw = null;
        try {
            File ins = new File(source);
            File outs = new File(destination);
            if (!outs.exists()) {
                outs.createNewFile();
            }
            br = new BufferedReader(new InputStreamReader(new FileInputStream(ins)));
            pw = new PrintWriter(outs);
            String line = "";
            while (true) {
                //读取一个属性
                line = br.readLine();
                if (line == null) {
                    break;
                }
                for (Map.Entry<String, String> entry : props.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    if (line.contains(key)) {
                        line = key + "=" + value;
                    }
                }
                pw.println(line);
            }
            pw.flush();
        } catch (Exception e) {
            throw e;
        } finally {
            try {
                close(br);
                close(pw);
            } catch (Exception e) {
            }
        }
    }

    public static void close(Object obj) throws Exception {
        if (obj == null) return;
        try {
            if (obj instanceof BufferedReader) ((BufferedReader) obj).close();
            if (obj instanceof PrintWriter) ((PrintWriter) obj).close();
            if (obj instanceof FileInputStream) ((FileInputStream) obj).close();
            if (obj instanceof InputStreamReader) ((InputStreamReader) obj).close();

        } catch (Exception e) {
            System.out.println("FileUtil close method error.");
            throw e;
        }
    }

    public static void readFile(String fileName, BufferedWriter bw) throws Exception {
        //部署成功，在前台打印出结果
        File paramFile = new File(fileName);
        FileInputStream fins = null;
        InputStreamReader insr = null;
        BufferedReader br = null;
        try {
            fins = new FileInputStream(paramFile);
            insr = new InputStreamReader(fins);
            br = new BufferedReader(insr);
            String line = "";
            while ((line = br.readLine()).length() > 0) {
                writeAndPrint(line);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            close(fins);
            close(insr);
            close(br);
        }
    }

    public static void writeAndPrint(String log) throws Exception {
        bw.write(log + "\n");
        System.out.println(log);
    }
}
