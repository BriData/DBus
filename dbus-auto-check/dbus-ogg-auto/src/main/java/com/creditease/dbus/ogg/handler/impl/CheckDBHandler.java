package com.creditease.dbus.ogg.handler.impl;

import com.creditease.dbus.ogg.bean.ConfigBean;
import com.creditease.dbus.ogg.container.AutoCheckConfigContainer;
import com.creditease.dbus.ogg.handler.AbstractHandler;
import com.creditease.dbus.ogg.utils.DBUtil;

import java.io.BufferedWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * User: 王少楠
 * Date: 2018-08-24
 * Desc:
 */
public class CheckDBHandler extends AbstractHandler {

    public void checkDeploy(BufferedWriter bw)throws Exception {
        checkDB(bw);
    }

    private void checkDB(BufferedWriter bw) throws Exception{
        System.out.println("============================================");
        System.out.println("check db account start ");
        bw.write("============================================");
        bw.newLine();
        bw.write("check db account start ");
        bw.newLine();


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
            if(rs.next()){
                System.out.println("ogg 用户拥有权限："+rs.getString("ROLE"));
                System.out.println("check db account ok ");
                System.out.println("============================================");

                bw.write("check db account ok ");
                bw.newLine();
                bw.write("============================================");
                bw.newLine();
            }else {
                System.out.println("check db account ok fail: ogg 用户未授权DBA权限！！");
                System.out.println("============================================");
                bw.write("check db account ok fail: ogg 用户未授权DBA权限！！");
                bw.newLine();
                bw.write("============================================");
                bw.newLine();
                throw new Exception("check fail");
            }


        }catch (Exception e){
            System.out.println("check db account fail.");
            bw.write("check db account fail.");
            bw.newLine();
            throw e;
        }finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
        }
    }
}
