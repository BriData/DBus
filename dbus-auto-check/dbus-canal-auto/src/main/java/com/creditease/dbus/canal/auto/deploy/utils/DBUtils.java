package com.creditease.dbus.canal.auto.deploy.utils;

import com.creditease.dbus.canal.auto.deploy.bean.DeployPropsBean;

import java.io.BufferedWriter;
import java.sql.*;

/**
 * User: 王少楠
 * Date: 2018-08-10
 * Desc:
 */
public class DBUtils {
    public static  void checkDBAccount(DeployPropsBean deployProps, BufferedWriter bw) throws Exception{
        try {
            bw.write("-----check database canal account  begin ");
            bw.newLine();
            bw.write("canal user: " + deployProps.getCanalUser());
            bw.newLine();
            bw.write("canal pwd: " + deployProps.getCanalPwd());
            bw.newLine();
            bw.write("slave url: " + deployProps.getSlavePath());
            bw.newLine();
            Connection conn = getConn(deployProps.getSlavePath(),
                    deployProps.getCanalUser(), deployProps.getCanalPwd(), bw);
            if (!conn.isClosed()) {
                bw.write("-----check database canal account success");
                bw.newLine();
                closeAll(conn, null, null, bw);
            } else {
                closeAll(conn, null, null, bw);
                throw new Exception();
            }
        }catch (Exception e){
            bw.write("-----check database canal account fail ------------");
            bw.newLine();
            throw e;
        }
    }

    public static Connection getConn(String url,String user,String pwd,BufferedWriter bw) throws Exception {
        String driver = "com.mysql.jdbc.Driver";
        try{
            Class.forName(driver);
            Connection conn = DriverManager.getConnection(url,user,pwd);
            return conn;
        }catch (ClassNotFoundException e){
            bw.write("[jdbc ]get connection error");
            bw.newLine();
            throw e;
        }catch (SQLException e){
            bw.write("[jdbc ]get connection error");
            bw.newLine();
            throw e;
        }
    }

    public static void closeAll(Connection conn, PreparedStatement ps, ResultSet rs, BufferedWriter bw) throws Exception{
        try{
            if(rs!=null){
                rs.close();
            }
            if(ps!=null){
                ps.close();
            }
            if(conn!=null){
                conn.close();
            }
        }catch(SQLException e){
            bw.write("[jdbc ]close error");
            throw e;
        }
    }
}
