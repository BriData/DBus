package com.creditease.dbus.canal.auto.deploy.utils;

import com.creditease.dbus.canal.auto.deploy.bean.DeployPropsBean;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * User: 王少楠
 * Date: 2018-08-07
 * Desc:
 */
public class FileUtils {

    public static DeployPropsBean readProps(String path,BufferedWriter bw) throws IOException {
        try {
            Properties deployProps = new Properties();
            InputStream ins = new BufferedInputStream(new FileInputStream(path));
            deployProps.load(ins);
            DeployPropsBean props = new DeployPropsBean();
            props.setDsName(deployProps.getProperty("dsname").trim());
            props.setZkPath(deployProps.getProperty("zk.path").trim());
            props.setSlavePath(deployProps.getProperty("canal.address").trim());
            props.setCanalUser(deployProps.getProperty("canal.user").trim());
            props.setCanalPwd(deployProps.getProperty("canal.pwd").trim());

            ins.close();
            return props;
        }catch (Exception e){
            bw.write(" read props file error:  "+path);
            bw.newLine();
            throw e;
        }
    }

    public static void printDeployProps(DeployPropsBean deployProps, BufferedWriter bw) throws Exception{
        bw.write("数据源名称: "+deployProps.getDsName());
        bw.newLine();
        bw.write("zk地址: "+deployProps.getZkPath());
        bw.newLine();
        bw.write("备库地址: "+deployProps.getSlavePath());
        bw.newLine();
        bw.write("canal 用户名: "+deployProps.getCanalUser());
        bw.newLine();
        bw.write("canal 密码: "+deployProps.getCanalPwd());
        bw.newLine();
    }

    /**
     * 更新properties文件某key的value
     */
    public static void WriteProperties (String filePath, String pKey, String pValue,BufferedWriter bw) throws IOException {
       /* 直接用properties读写，会消除原来文件的注释和格式
       Properties props = new Properties();
        InputStream in = new FileInputStream(filePath);
        props.load(in);

        OutputStream out = new FileOutputStream(filePath);
        props.setProperty(pKey,pValue);
        props.store(out,"");

        in.close();
        out.close();*/
       try {
           File props = new File(filePath);
           BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(props)));

           List<String> fileContent = new LinkedList();
           while (true) {
               String line = br.readLine();
               if (line == null)
                   break;
               if (line.contains(pKey)) {
                   fileContent.add(pKey + "=" + pValue);
               } else {
                   fileContent.add(line);
               }
           }

           br.close();

           PrintWriter pw = new PrintWriter(props);
           for (String line : fileContent) {
               pw.println(line);
           }
           pw.close();
           bw.write("props: "+pKey+"="+pValue);
           bw.newLine();
       }catch (Exception e){
           bw.write(" write props error:  file: "+filePath+";  key: "+pKey+"; value"+pValue);
           bw.newLine();
           throw e;
       }
    }

    public static void copyFile(String source, String dest,BufferedWriter bw )throws Exception{
        FileChannel input = null;
        FileChannel output= null;
        try {
            File sourceFile = new File(source);
            if(!sourceFile.exists()){
                bw.write("file doesn't exist. path: "+source);
                bw.newLine();
                throw new Exception();
            }
            input = new FileInputStream(source).getChannel();
            output = new FileOutputStream(dest).getChannel();
            output.transferFrom(input,0,input.size());
        }catch (Exception e){
            bw.write("copy fail...");
            bw.newLine();
            throw e;
        }finally {
            input.close();
            output.close();
        }
    }

}
