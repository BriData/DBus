package com.creditease.dbus.ogg.utils;

import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.*;

/**
 * User: 王少楠
 * Date: 2018-08-24
 * Desc:
 */
public class FileUtil {
    private FileUtil(){}

    /**
     * 更新properties文件某key的value
     */
    public static void WriteProperties (String source, Properties props, String destination, BufferedWriter bw) throws IOException {
       /* 直接用properties读写，会消除原来文件的注释和格式
       Properties props = new Properties();
        InputStream in = new FileInputStream(filePath);
        props.load(in);

        OutputStream out = new FileOutputStream(filePath);
        props.setProperty(pKey,pValue);
        props.store(out,"");

        in.close();
        out.close();*/

        File ins = null;
        BufferedReader br = null;
        File outs = null;
        PrintWriter pw = null;
        try {
            ins = new File(source);
            br = new BufferedReader(new InputStreamReader(new FileInputStream(ins)));

            List<String> fileContent = new LinkedList();
            Set pKeys = props.keySet();


            while (true) {
                //读取一个属性
                String line = br.readLine();
                if (line == null)
                    break;
                //查看这个属性是否需要更新
                boolean toUpdate = false;
                Iterator iterator = pKeys.iterator();
                String pKey = StringUtils.EMPTY;
                String pValue = StringUtils.EMPTY;
                while(iterator.hasNext()){
                    pKey= (String) iterator.next();
                    pValue = props.getProperty(pKey);
                    if (line.contains(pKey)) {
                      toUpdate = true;
                      break;
                    }

                }
                if(toUpdate){
                    fileContent.add(pKey + "=" + pValue);
                    System.out.println("设置属性： "+pKey+"="+pValue);
                    bw.write("设置属性： "+pKey+"="+pValue);
                    bw.newLine();
                }else {
                    fileContent.add(line);
                }
            }

            br.close();

            outs = new File(destination);
            if(!outs.exists()){
                outs.createNewFile();
            }

            pw = new PrintWriter(outs);
            for (String line : fileContent) {
                pw.println(line);
            }
            pw.flush();
            pw.close();

        }catch (Exception e){
            throw e;
        }finally {
            try {
                close(br);
                close(pw);
            }catch (Exception e){
            }
        }
    }

    public static void close(Object obj) throws Exception{
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

    public static void readFile(String fileName,BufferedWriter bw) throws  Exception{
        //部署成功，在前台打印出结果
        File paramFile = new File(fileName);
        FileInputStream fins = null;
        InputStreamReader insr = null;
        BufferedReader br =null;
        try {
            fins = new FileInputStream(paramFile);
            insr = new InputStreamReader(fins);
            br =new BufferedReader(insr);
            String line = br.readLine();
            while (line != null) {
                System.out.println(line);
                bw.write(line);
                bw.newLine();

                line=br.readLine();
            }
        }catch (Exception e){
            throw e;
        }finally {
           close(fins);
           close(insr);
           close(br);
        }

    }
}
