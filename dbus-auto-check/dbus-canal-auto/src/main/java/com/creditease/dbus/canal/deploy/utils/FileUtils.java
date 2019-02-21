package com.creditease.dbus.canal.deploy.utils;

import com.creditease.dbus.canal.deploy.bean.DeployPropsBean;

import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * User: 王少楠
 * Date: 2018-08-07
 * Desc:
 */
public class FileUtils {

    public static DeployPropsBean readProps(String path, BufferedWriter bw) throws Exception {
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
        } catch (Exception e) {
            writeAndPrint(bw, " read props file error:  " + path);
            throw e;
        }
    }

    /**
     * 更新properties文件某key的value
     */
    public static void writeProperties(String filePath, String key, String value, BufferedWriter bw) throws Exception {
        try {
            Boolean over = false;
            File props = new File(filePath);
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(props)));

            List<String> fileContent = new LinkedList();
            while (true) {
                String line = br.readLine();
                if (line == null)
                    break;
                if (!over && line.contains(key)) {
                    fileContent.add(value);
                    over = true;
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
            writeAndPrint(bw, "props: " + value);

        } catch (Exception e) {
            writeAndPrint(bw, " write props error:  file: " + filePath + "properties:" + value);
            throw e;
        }
    }

    public static void writeAndPrint(BufferedWriter bw, String log) throws Exception {
        bw.write(log + "\n");
        System.out.println(log);
    }

}
