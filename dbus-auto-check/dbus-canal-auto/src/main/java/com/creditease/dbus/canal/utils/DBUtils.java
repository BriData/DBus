package com.creditease.dbus.canal.utils;

import com.creditease.dbus.canal.bean.DeployPropsBean;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedWriter;
import java.sql.*;

import static com.creditease.dbus.canal.utils.FileUtils.writeAndPrint;

/**
 * User: 王少楠
 * Date: 2018-08-10
 * Desc:
 */
public class DBUtils {
    public static void checkDBAccount(DeployPropsBean deployProps) throws Exception {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            writeAndPrint( "*************************** CHECK DATABASE CANAL ACCOUNT  BEGIN *****************************");
            writeAndPrint( "canal user: " + deployProps.getCanalUser());
            writeAndPrint( "canal pwd: " + deployProps.getCanalPwd());

            String url = "jdbc:mysql://" + deployProps.getSlavePath() + "/dbus?characterEncoding=utf-8";
            writeAndPrint( "slave url: " + url);

            conn = getConn(url, deployProps.getCanalUser(), deployProps.getCanalPwd());
            if (!conn.isClosed()) {
                writeAndPrint( "数据库连接成功...");
                writeAndPrint( "检查blog format: show variables like '%bin%'");
                ps = conn.prepareStatement("show variables like '%bin%'");
                rs = ps.executeQuery();

                String binlogFormat = null;
                while (rs.next()) {
                    if (binlogFormat != null) {
                        break;
                    }
                    String varName = rs.getString(1);
                    if (StringUtils.equals("binlog_format", varName)) {
                        binlogFormat = rs.getString(2);
                    }
                }
                if (binlogFormat == null) {
                    writeAndPrint( "检查失败，请确认bin format及同步模式");
                    throw new Exception();
                } else {
                    writeAndPrint( "binlog_format : " + binlogFormat);
                    if (!"ROW".equals(binlogFormat.trim())) {
                        writeAndPrint( "检查失败，请确认bin format及同步模式");
                        throw new Exception();
                    }
                }
                writeAndPrint( "****************************** CHECK DATABASE CANAL ACCOUNT SUCCESS *************************");
            } else {
                writeAndPrint( "数据库检查连接失败，请检查canal用户名、密码以及源端库备库地址！");
                throw new Exception();
            }
        } catch (Exception e) {
            writeAndPrint( "数据库检查失败，请确认以下配置");
            writeAndPrint( "1.canal用户名、密码以及源端库备库地址。2.canal用户的授权。3.源端库主库和备库binlog相关设置");
            writeAndPrint( "****************************** CHECK DATABASE CANAL ACCOUNT FAIL ****************************");
            throw e;
        } finally {
            closeAll(conn, ps, rs);
        }
    }

    public static Connection getConn(String url, String user, String pwd) throws Exception {
        String driver = "com.mysql.jdbc.Driver";
        try {
            Class.forName(driver);
            //设置超时时间10s
            DriverManager.setLoginTimeout(10);
            Connection conn = DriverManager.getConnection(url, user, pwd);
            return conn;
        } catch (ClassNotFoundException e) {
            writeAndPrint( "[jdbc ]get connection error");
            throw e;
        } catch (SQLException e) {
            writeAndPrint( "[jdbc ]get connection error");
            throw e;
        }
    }

    public static void closeAll(Connection conn, PreparedStatement ps, ResultSet rs) throws Exception {
        try {
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            writeAndPrint( "[jdbc ]close error");
            throw e;
        }
    }
}
