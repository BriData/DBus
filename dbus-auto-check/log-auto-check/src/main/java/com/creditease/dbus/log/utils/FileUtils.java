/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2019 Bridata
 * ==
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * >>
 */


package com.creditease.dbus.log.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;

public class FileUtils {
    private static Logger logger = LoggerFactory.getLogger(FileUtils.class);

    /**
     * 实现向指定位置
     * 插入数据
     *
     * @param fileName      文件名
     * @param points        指针位置
     * @param insertContent 插入内容
     **/
    public static void insert(String fileName, long points, String insertContent) {
        try {
            File tmp = File.createTempFile("tmp", null);
            tmp.deleteOnExit();//在JVM退出时删除

            RandomAccessFile raf = new RandomAccessFile(fileName, "rw");
            //创建一个临时文件夹来保存插入点后的数据
            FileOutputStream tmpOut = new FileOutputStream(tmp);
            FileInputStream tmpIn = new FileInputStream(tmp);
            raf.seek(points);

            //将插入点后的内容读入临时文件夹
            byte[] buff = new byte[10240];
            //用于保存临时读取的字节数
            int hasRead = 0;
            //循环读取插入点后的内容
            while ((hasRead = raf.read(buff)) > 0) {
                // 将读取的数据写入临时文件中
                tmpOut.write(buff, 0, hasRead);
            }
            //返回原来的插入处
            raf.seek(points);
            //追加需要追加的内容
            raf.write(insertContent.getBytes());
            //最后追加临时文件中的内容
            while ((hasRead = tmpIn.read(buff)) > 0) {
                raf.write(buff, 0, hasRead);
            }
            System.out.println("update config: [filebeat.extract.file.path: " + fileName + "] success!\n");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void modifyFileProperties(String filePath, String oldConfig, String newConfig, BufferedWriter bw) throws IOException {
        try {
            File file = new File(filePath);
            FileReader fileReader = new FileReader(file);
            BufferedReader br = new BufferedReader(fileReader);

            List<String> fileContent = new LinkedList();
            while (true) {
                String line = br.readLine();
                if (line == null)
                    break;
                if (line.contains(oldConfig)) {
                    fileContent.add(newConfig);
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
            bw.write("新配置项: " + newConfig);
            System.out.println("更新配置: [" + newConfig + "] 成功!\n");
        } catch (Exception e) {
            bw.write(" 更新文件配置项错误:  文件: " + filePath + ";  新配置项: " + newConfig + "\n");
            System.out.println("更新配置项错误: 新配置项: [" + newConfig + "]");
            System.out.println("详细错误信息: " + e);
            throw e;
        }
    }

    //删除文件
    public static void deleteFile(String filePath) {
        File delFile = new File(filePath);
        delFile.delete();
    }

    //复制文件，java NIO复制方法，更快速
    public static void copyFileUsingFileChannels(File source, File dest) throws IOException {
        FileChannel inputChannel = null;
        FileChannel outputChannel = null;
        try {
            inputChannel = new FileInputStream(source).getChannel();
            outputChannel = new FileOutputStream(dest).getChannel();
            outputChannel.transferFrom(inputChannel, 0, inputChannel.size());
        } finally {
            inputChannel.close();
            outputChannel.close();
        }
    }

    //检测文件及文件夹是否存在
    public static boolean checkFileAndFolderIsExist(String path) {

        if (!StringUtils.startsWith(path, "/")) {
            System.out.println("ERROR: 文件或文件夹路径：" + path + " is not beginning with '/'! 请检查配置文件 [conf/log-conf.properties]");
            return false;
        }

        String[] arr = StringUtils.split(path, "/");
        String basePath = "";
        int len = arr.length;
        if (StringUtils.contains(arr[len - 1], "*")) {
            len = len - 1;
        }

        for (int i = 0; i <= len - 1; i++) {
            if (i == 0) {
                String filePath = "/" + arr[0];
                basePath = filePath;
                File file = new File(filePath);
                if (!file.exists()) {
                    System.out.println("ERROR: 文件或文件夹路径：" + filePath + " is not exist! 请检查配置文件 [conf/log-conf.properties]");
                    return false;
                }
            } else {
                String filePath = StringUtils.joinWith("/", basePath, arr[i]);
                basePath = filePath;
                File file = new File(filePath);
                if (!file.exists()) {
                    System.out.println("ERROR: 文件或文件夹路径：" + filePath + " is not exist! 请检查配置文件 [conf/log-conf.properties]");
                    return false;
                }
            }
        }
        return true;
    }


    public static void main(String[] args) throws IOException {


    }
}
