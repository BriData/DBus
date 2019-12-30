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


package com.creditease.dbus.utils;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * This is Description
 *
 * @author xiancangao
 * @date 2018/12/14
 */
public class SSHUtils {
    private static Logger logger = LoggerFactory.getLogger(SSHUtils.class);

    public static String uploadFile(String user, String host, int port, String pubKeyPath, String pathFrom, String pathTo) {
        Session session = null;
        ChannelSftp channel = null;
        InputStream in = null;
        try {
            logger.info("will upload file:{} to host:{}, path:{}", pathFrom, host, pathTo);
            JSch jsch = new JSch();
            jsch.addIdentity(pubKeyPath);

            session = jsch.getSession(user, host, port);
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect(30000);

            channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect(1000);
            channel.cd(pathTo);
            File file = new File(pathFrom);
            in = new FileInputStream(file);
            channel.put(in, file.getName());
            return "ok";
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return e.getMessage();
        } finally {
            try {
                if (session != null) {
                    session.disconnect();
                }
                if (channel != null) {
                    channel.disconnect();
                }
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    /**
     * @param user
     * @param host
     * @param port
     * @param pubKeyPath
     * @param command
     * @param error      ture只返回错误信息,false只返回正常信息,null错误正常都返回
     * @return
     */
    public static String executeCommand(String user, String host, Integer port, String pubKeyPath, String command, Boolean error) {
        logger.info("user:{},host:{},port:{},keyPath:{},command:{}", user, host, port, pubKeyPath, command);
        Session session = null;
        ChannelExec channel = null;
        InputStream is = null;
        InputStream es = null;
        try {
            JSch jsch = new JSch();
            jsch.addIdentity(pubKeyPath);

            session = jsch.getSession(user, host, port);
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();
            channel = (ChannelExec) session.openChannel("exec");
            channel.setCommand(command);
            channel.setInputStream(null);

            is = channel.getInputStream();
            es = channel.getErrStream();
            channel.connect();
            StringBuilder inputMsg = new StringBuilder();
            StringBuilder errorMsg = new StringBuilder();
            byte[] tmp = new byte[1024];
            while (true) {
                while (is.available() > 0) {
                    int i = is.read(tmp, 0, 1024);
                    if (i < 0) break;
                    inputMsg.append(new String(tmp, 0, i));
                }
                while (es.available() > 0) {
                    int i = es.read(tmp, 0, 1024);
                    if (i < 0) break;
                    errorMsg.append(new String(tmp, 0, i));
                }
                if (channel.isClosed()) {
                    if ((is.available() > 0) || (es.available() > 0)) continue;
                    break;
                }
                Thread.sleep(1000);
            }
            //logger.info("inputMsg:{}", inputMsg.toString());
            logger.info("errorMsg:{}", errorMsg.toString());
            if (error == null) {
                return inputMsg.toString().length() > 0 ? inputMsg.toString() : errorMsg.toString();
            }
            if (error) {
                return errorMsg.toString();
            } else {
                return inputMsg.toString();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null;
        } finally {
            try {
                if (channel != null) {
                    channel.disconnect();
                }
                if (session != null) {
                    session.disconnect();
                }
                if (is != null) {
                    is.close();
                }
                if (es != null) {
                    es.close();
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    public static void main(String[] args) {
        String app = executeCommand("app", "vdbus-19", 22, "C:\\Users\\admin\\.ssh\\id_rsa",
                "mkdir -pv /app/dbus/keeper_test/test/11", null);
        System.out.println("======");
        System.out.println(StringUtils.isNotBlank(app));
        System.out.println(app);
    }
}
