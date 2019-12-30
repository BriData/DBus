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


package com.creditease.dbus.service;

import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.base.com.creditease.dbus.utils.RequestSender;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.constant.ServiceNames;
import com.creditease.dbus.domain.model.User;
import com.creditease.dbus.mail.DBusMailFactory;
import com.creditease.dbus.mail.IMail;
import com.creditease.dbus.mail.Message;
import com.creditease.dbus.utils.DBusUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;


/**
 * Created by xiancangao on 2018/05/31
 */
@Service
public class ConfigCenterService {

    @Autowired
    private IZkService zkService;
    @Autowired
    private ZkConfService zkConfService;
    @Autowired
    private RequestSender sender;
    @Autowired
    private DBusInitService initService;

    private Logger logger = LoggerFactory.getLogger(getClass());

    public ResultEntity updateGlobalConf(LinkedHashMap<String, String> map) {
        ResultEntity resultEntity = new ResultEntity();
        try {
            resultEntity = initService.checkParams(resultEntity, map);
            if (resultEntity.getStatus() != 0) {
                return resultEntity;
            }
            //以下处理保留额外添加的特殊配置
            Properties properties = zkService.getProperties(Constants.GLOBAL_PROPERTIES_ROOT);
            properties.putAll(map);
            StringBuilder sb = new StringBuilder();
            map.forEach((k, v) -> sb.append(k).append("=").append(v).append("\n"));
            zkService.setData(Constants.GLOBAL_PROPERTIES_ROOT, sb.toString().getBytes("utf-8"));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            resultEntity.setStatus(MessageCode.EXCEPTION);
            resultEntity.setMessage(e.getMessage());
        }
        return resultEntity;
    }

    public ResultEntity updateMgrDB(Map<String, String> map) throws Exception {
        ResultEntity resultEntity = new ResultEntity();
        Connection connection = null;
        try {
            String content = map.get("content");
            map.clear();
            String[] split = content.split("\n");
            for (String s : split) {
                String replace = s.replace("\r", "");
                String[] pro = replace.split("=", 2);
                if (pro != null && pro.length == 2) {
                    map.put(pro[0], pro[1]);
                }
            }
            logger.info(map.toString());
            String driverClassName = map.get("driverClassName");
            String url = map.get("url");
            String username = map.get("username");
            String password = map.get("password");
            Class.forName(driverClassName);
            connection = DriverManager.getConnection(url, username, password);

            zkService.setData(KeeperConstants.MGR_DB_CONF, content.getBytes("utf-8"));
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            resultEntity.setStatus(MessageCode.DBUS_MGR_DB_FAIL_WHEN_CONNECT);
            resultEntity.setMessage(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
        return resultEntity;
    }

    public ResultEntity getBasicConf() {
        return sender.get(ServiceNames.KEEPER_SERVICE, "/toolSet/getMgrDBMsg").getBody();
    }

    public Boolean isInitialized() throws Exception {
        return zkService.isExists(Constants.DBUS_ROOT);
    }

    public boolean urlTest(String url) {
        boolean result = false;
        HttpURLConnection conn = null;
        try {
            URL url_ = new URL(url);
            conn = (HttpURLConnection) url_.openConnection();
            int code = conn.getResponseCode();
            if (code == 200) {
                result = true;
            }
        } catch (Exception e) {
            logger.error("url连通性测试异常.errorMessage:{},url{}", e.getMessage(), url, e);
        } finally {
            if (conn == null) {
                conn.disconnect();
            }
        }
        return result;
    }

    public boolean urlTest(String host, int port) {
        boolean result = false;
        Socket socket = null;
        try {
            socket = new Socket();
            socket.connect(new InetSocketAddress(host, port));
            socket.close();
            result = true;
        } catch (IOException e) {
            logger.error("连通性测试异常.errorMessage:{};host:{},port:{}", e.getMessage(), host, port, e);
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
        return result;
    }

    public int ResetMgrDB(LinkedHashMap<String, String> map) throws Exception {
        Connection connection = null;
        try {
            String content = map.get("content");
            map.clear();
            String[] split = content.split("\n");
            for (String s : split) {
                String replace = s.replace("\r", "");
                String[] pro = replace.split("=", 2);
                if (pro != null && pro.length == 2) {
                    map.put(pro[0], pro[1]);
                }
            }
            logger.info(map.toString());
            String driverClassName = map.get("driverClassName");
            String url = map.get("url");
            String username = map.get("username");
            String password = map.get("password");
            Class.forName(driverClassName);
            connection = DriverManager.getConnection(url, username, password);

            zkService.setData(KeeperConstants.MGR_DB_CONF, content.getBytes("utf-8"));

            //重置mgr数据库
            ResponseEntity<ResultEntity> res = sender.get(ServiceNames.KEEPER_SERVICE, "/toolSet/initMgrSql");
            if (res.getBody().getStatus() != 0) {
                return MessageCode.DBUS_MGR_INIT_ERROR;
            }
            logger.info("重置mgr数据库完成.");

            //超级管理员添加
            User u = new User();
            u.setRoleType("admin");
            u.setStatus("active");
            u.setUserName("超级管理员");
            u.setPassword(DBusUtils.md5("12345678"));
            u.setEmail("admin");
            u.setPhoneNum("13000000000");
            u.setUpdateTime(new Date());
            res = sender.post(ServiceNames.KEEPER_SERVICE, "/users/create", u);
            if (res.getBody().getStatus() != 0) {
                return MessageCode.CREATE_SUPER_USER_ERROR;
            }
            logger.info("添加超级管理员完成.");
            return 0;
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            return MessageCode.DBUS_MGR_DB_FAIL_WHEN_CONNECT;
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    public boolean sendMailTest(Map<String, Object> map) {
        IMail mail = DBusMailFactory.build();
        Message msg = new Message();
        msg.setAddress((String) map.get("adminEmail"));
        msg.setContents("测试邮件");
        msg.setSubject("测试邮件");

        msg.setHost((String) map.get("alarmMailSMTPAddress"));
        msg.setPort(Integer.valueOf((String) map.get("alarmMailSMTPPort")));
        msg.setUserName((String) map.get("alarmMailUser"));
        msg.setPassword((String) map.get("alarmMailPass"));
        msg.setFromAddress((String) map.get("alarmSendEmail"));
        return mail.send(msg);
    }
}
