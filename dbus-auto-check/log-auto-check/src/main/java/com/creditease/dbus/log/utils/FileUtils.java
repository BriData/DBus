package com.creditease.dbus.log.utils;

import org.apache.commons.lang3.SystemUtils;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class FileUtils {
    /**
     * 实现向指定位置
     * 插入数据
     * @param fileName 文件名
     * @param points 指针位置
     * @param insertContent 插入内容
     * **/
    public static void insert(String fileName, long points, String insertContent){
        try{
            File tmp = File.createTempFile("tmp", null);
            tmp.deleteOnExit();//在JVM退出时删除

            RandomAccessFile raf = new RandomAccessFile(fileName, "rw");
            //创建一个临时文件夹来保存插入点后的数据
            FileOutputStream tmpOut = new FileOutputStream(tmp);
            FileInputStream tmpIn = new FileInputStream(tmp);
            raf.seek(points);

            //将插入点后的内容读入临时文件夹
            byte [] buff = new byte[10240];
            //用于保存临时读取的字节数
            int hasRead = 0;
            //循环读取插入点后的内容
            while((hasRead = raf.read(buff)) > 0){
                // 将读取的数据写入临时文件中
                tmpOut.write(buff, 0, hasRead);
            }

            //插入需要指定添加的数据
            raf.seek(points);//返回原来的插入处
            //追加需要追加的内容
            raf.write(insertContent.getBytes());
            //最后追加临时文件中的内容
            while((hasRead = tmpIn.read(buff)) > 0){
                raf.write(buff,0,hasRead);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }


    public static void modifyFileProperties(String filePath, String oldStr, String newStr, BufferedWriter bw) throws IOException {
        try {
            File file = new File(filePath);
            FileReader fileReader = new FileReader(file);
            BufferedReader br = new BufferedReader(fileReader);

            List<String> fileContent = new LinkedList();
            while (true) {
                String line = br.readLine();
                if (line == null)
                    break;
                if (line.contains(oldStr)) {
                    fileContent.add(newStr);
                } else {
                    fileContent.add(line);
                }
            }

            br.close();

            PrintWriter pw = new PrintWriter(file);
            for (String line : fileContent) {
                pw.println(line);
            }
            pw.close();
            bw.write("props: " + newStr);
            bw.newLine();
        } catch (Exception e){
            bw.write(" write props error:  file: " + filePath + ";  props: " + newStr);
            bw.newLine();
            throw e;
        }
    }


    public static void main(String[] args) throws IOException {
//        String insertContent = "\n- type: log\n" +
//                "  enabled: true\n" +
//                "  paths:\n" +
//                "    - /app/jar/logs/vdbus-16/track_log/track-event*.log\n" +
//                "  fields_under_root: true\n" +
//                "  fields:\n" +
//                "    type: data_log\n" +
//                "  encoding: utf-8";
//        insert("D:\\dbus_project\\dbus\\dbus\\dbus-main\\dbus-auto-check\\dbus-log-check\\conf\\filebeat.yml",22,insertContent);

        File userDir = new File(SystemUtils.USER_DIR.replaceAll("\\\\", "/"));
        File outDir = new File(userDir, "reports");
        if (!outDir.exists()) outDir.mkdirs();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String strTime = sdf.format(new Date());

        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        BufferedWriter bw = null;

        File file = new File(outDir, "check_report_" + strTime + ".txt");

        fos = new FileOutputStream(file);
        osw = new OutputStreamWriter(fos);
        bw = new BufferedWriter(osw);
        modifyFileProperties("D:\\dbus_project\\dbus\\dbus\\dbus-main\\dbus-auto-check\\dbus-log-check\\conf\\flume-conf.properties", "host", "agent.sources.r_hb_0.interceptors.i_sr_2.replaceString={\\\"message\\\":\\\"$1\\\", \\\"type\\\":\\\"dbus_log\\\", \\\"host\\\":\\\"10.120.64.175\\\"}\n", bw);
//        modifyFileProperties("D:\\dbus_project\\dbus\\dbus\\dbus-main\\dbus-auto-check\\dbus-log-check\\conf\\flume-conf.properties", "host", "aa\n", bw);


    }
}