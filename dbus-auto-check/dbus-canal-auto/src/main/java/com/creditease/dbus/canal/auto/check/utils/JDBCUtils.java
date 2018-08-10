package com.creditease.dbus.canal.auto.check.utils;

import java.io.BufferedWriter;
import java.sql.*;

/**
 * User: 王少楠
 * Date: 2018-08-06
 * Desc:
 */
public class JDBCUtils {

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

    public static void closeAll(Connection conn, PreparedStatement ps, ResultSet rs,BufferedWriter bw) throws Exception{
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
