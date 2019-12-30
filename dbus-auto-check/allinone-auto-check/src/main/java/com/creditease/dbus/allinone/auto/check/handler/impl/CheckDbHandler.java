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


package com.creditease.dbus.allinone.auto.check.handler.impl;

import com.creditease.dbus.allinone.auto.check.bean.AutoCheckConfigBean;
import com.creditease.dbus.allinone.auto.check.container.AutoCheckConfigContainer;
import com.creditease.dbus.allinone.auto.check.handler.AbstractHandler;
import com.creditease.dbus.allinone.auto.check.utils.DBUtils;
import com.creditease.dbus.allinone.auto.check.utils.MsgUtil;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created by Administrator on 2018/8/1.
 */
public class CheckDbHandler extends AbstractHandler {

    @Override
    public void check(BufferedWriter bw) throws Exception {
        checkDbusMgr(bw);
        checkDbus(bw);
        checkCanal(bw);
        checkTestSchema(bw);
    }

    private void checkDbusMgr(BufferedWriter bw) throws Exception {
        bw.write("check db&user dbusmgr start:");
        bw.newLine();
        bw.write("============================================");
        bw.newLine();
        AutoCheckConfigBean conf = AutoCheckConfigContainer.getInstance().getAutoCheckConf();
        String[] sqls = StringUtils.split(conf.getDbDbusmgrTestSql(), ",");
        for (String sql : sqls) {
            Connection conn = null;
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                String[] arrs = StringUtils.split(sql, "\\.");
                String url = MsgUtil.format("jdbc:mysql://{0}:{1}/{2}?characterEncoding=utf-8", conf.getDbDbusmgrHost(), String.valueOf(conf.getDbDbusmgrPort()), arrs[0]);
                conn = DBUtils.getConn(url, conf.getDbDbusmgrSchema(), conf.getDbDbusmgrPassword());
                String sqlWk = MsgUtil.format("select count(*) cnt from {0}", arrs[1]);
                ps = conn.prepareStatement(sqlWk);
                rs = ps.executeQuery();
                if (rs.next()) {
                    bw.write("table " + arrs[1] + " data count: " + rs.getString("cnt"));
                    bw.newLine();
                }
            } catch (Exception e) {
                throw new RuntimeException("db&user dbusmgr check fail", e);
            } finally {
                DBUtils.close(rs);
                DBUtils.close(ps);
                DBUtils.close(conn);
            }
        }
        bw.newLine();
    }

    private void checkDbus(BufferedWriter bw) throws Exception {
        bw.write("check db&user dbus start:");
        bw.newLine();
        bw.write("============================================");
        bw.newLine();
        AutoCheckConfigBean conf = AutoCheckConfigContainer.getInstance().getAutoCheckConf();
        String[] sqls = StringUtils.split(conf.getDbDbusTestSql(), ",");
        for (String sql : sqls) {
            Connection conn = null;
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                String[] arrs = StringUtils.split(sql, "\\.");
                String url = MsgUtil.format("jdbc:mysql://{0}:{1}/{2}?characterEncoding=utf-8", conf.getDbDbusHost(), String.valueOf(conf.getDbDbusPort()), arrs[0]);
                conn = DBUtils.getConn(url, conf.getDbDbusSchema(), conf.getDbDbusPassword());
                String sqlWk = MsgUtil.format("select count(*) cnt from {0}", arrs[1]);
                ps = conn.prepareStatement(sqlWk);
                rs = ps.executeQuery();
                if (rs.next()) {
                    bw.write("table " + arrs[1] + " data count: " + rs.getString("cnt"));
                    bw.newLine();
                }
            } catch (Exception e) {
                throw new RuntimeException("db&user dbus check fail", e);
            } finally {
                DBUtils.close(rs);
                DBUtils.close(ps);
                DBUtils.close(conn);
            }
        }
        bw.newLine();
    }

    private void checkCanal(BufferedWriter bw) throws Exception {
        bw.write("check db&user canal start: ");
        bw.newLine();
        bw.write("============================================");
        bw.newLine();
        AutoCheckConfigBean conf = AutoCheckConfigContainer.getInstance().getAutoCheckConf();
        String[] sqls = StringUtils.split(conf.getDbCanalTestSql(), ",");
        int idx = 0;
        for (String sql : sqls) {
            Connection conn = null;
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                String[] arrs = StringUtils.split(sql, "\\.");
                String url = MsgUtil.format("jdbc:mysql://{0}:{1}/{2}?characterEncoding=utf-8", conf.getDbCanalHost(), String.valueOf(conf.getDbCanalPort()), arrs[0]);
                conn = DBUtils.getConn(url, conf.getDbCanalSchema(), conf.getDbCanalPassword());
                if (idx == 0) {
                    ps = conn.prepareStatement("show master status");
                    rs = ps.executeQuery();
                    if (rs.next()) {
                        bw.write(MsgUtil.format("master status File:{0}, Position:{1}", rs.getString("File"), rs.getString("Position")));
                        bw.newLine();
                    }
                    idx++;
                }
                String sqlWk = MsgUtil.format("select count(*) cnt from {0}", arrs[1]);
                ps = conn.prepareStatement(sqlWk);
                rs = ps.executeQuery();
                if (rs.next()) {
                    bw.write("table " + arrs[1] + " data count: " + rs.getString("cnt"));
                    bw.newLine();
                }
            } catch (Exception e) {
                throw new RuntimeException("db&user canal check fail", e);
            } finally {
                DBUtils.close(rs);
                DBUtils.close(ps);
                DBUtils.close(conn);
            }
        }
        bw.newLine();
    }

    private void checkTestSchema(BufferedWriter bw) throws Exception {
        bw.write("check db&user testschema start: ");
        bw.newLine();
        bw.write("============================================");
        bw.newLine();
        AutoCheckConfigBean conf = AutoCheckConfigContainer.getInstance().getAutoCheckConf();
        String[] sqls = StringUtils.split(conf.getDbTestSchemaTestSql(), ",");
        for (String sql : sqls) {
            Connection conn = null;
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                String[] arrs = StringUtils.split(sql, "\\.");
                String url = MsgUtil.format("jdbc:mysql://{0}:{1}/{2}?characterEncoding=utf-8", conf.getDbTestSchemaHost(), String.valueOf(conf.getDbTestSchemaPort()), arrs[0]);
                conn = DBUtils.getConn(url, conf.getDbTestSchemaSchema(), conf.getDbTestSchemaPassword());
                String sqlWk = MsgUtil.format("select count(*) cnt from {0}", arrs[1]);
                ps = conn.prepareStatement(sqlWk);
                rs = ps.executeQuery();
                if (rs.next()) {
                    bw.write("table " + arrs[1] + " data count: " + rs.getString("cnt"));
                    bw.newLine();
                }
            } catch (Exception e) {
                throw new RuntimeException("db&user testschema check fail", e);
            } finally {
                DBUtils.close(rs);
                DBUtils.close(ps);
                DBUtils.close(conn);
            }
        }
    }

}
