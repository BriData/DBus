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


package com.creditease.dbus.ogg.handler.impl;

import com.creditease.dbus.ogg.bean.ConfigBean;
import com.creditease.dbus.ogg.container.AutoCheckConfigContainer;
import com.creditease.dbus.ogg.handler.AbstractHandler;
import com.creditease.dbus.ogg.utils.DBUtil;

import java.io.BufferedWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static com.creditease.dbus.ogg.utils.FileUtil.writeAndPrint;

/**
 * User: 王少楠
 * Date: 2018-08-24
 * Desc:
 */
public class CheckDBHandler extends AbstractHandler {

    public void checkDeploy(BufferedWriter bw) throws Exception {
        checkDB(bw);
    }

    private void checkDB(BufferedWriter bw) throws Exception {
        writeAndPrint("********************************** CHECK DB ACCOUNT START ***********************************");
        writeAndPrint(" ");

        ConfigBean config = AutoCheckConfigContainer.getInstance().getConfig();
        String url = config.getOggUrl();
        String user = config.getOggUser();
        String pwd = config.getOggPwd();

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConn(url, user, pwd);
            String sqlTest = "select * from dba_roles where ROLE = 'DBA'";
            ps = conn.prepareStatement(sqlTest);
            rs = ps.executeQuery();
            if (rs.next()) {
                writeAndPrint("ogg 用户拥有权限：" + rs.getString("ROLE"));
                writeAndPrint("check db account ok ");
            } else {
                writeAndPrint("check db account fail: ogg 用户未授权DBA权限！！");
                throw new Exception();
            }
            writeAndPrint("********************************* CHECK DB ACCOUNT SUCCDESS *********************************");
        } catch (Exception e) {
            writeAndPrint("********************************** CHECK DB ACCOUNT FAIL ************************************");
            throw e;
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
    }
}
