package com.creditease.dbus.canal.auto.deploy.utils;

import com.creditease.dbus.canal.auto.deploy.bean.DeployPropsBean;
import org.apache.commons.lang3.StringUtils;

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
            //url
            String url = "jdbc:mysql://"+deployProps.getSlavePath()+"/dbus?characterEncoding=utf-8";
            bw.write("slave url: " + url);
            bw.newLine();
            Connection conn = getConn(url,
                    deployProps.getCanalUser(), deployProps.getCanalPwd(), bw);
            if (!conn.isClosed()) {
                bw.write("数据库连接成功...");
                bw.newLine();
                Statement stm = conn.createStatement();

                //mysql 命令
                String blogFormat = "show variables like '%bin%'";
                /*String slaveStatus = "show slave status";//备库是否正常
                String materStatus = "show master status"; //binlog是否正常*/


                ResultSet rs = stm.executeQuery(blogFormat);

                //检查bin log
                bw.write("检查blog format: "+ blogFormat+"...");
                bw.newLine();
                String binlogFormat = null;
                while(rs.next()){
                    if(binlogFormat != null){
                        break;
                    }
                    String varName = rs.getString(1);
                    if(StringUtils.equals("binlog_format",varName)){
                        binlogFormat = rs.getString(2);
                    }
                }
                if(binlogFormat == null ){
                    bw.write("检查失败，请确认bin format及同步模式...");
                    bw.newLine();
                    throw new Exception();
                }else{
                    bw.write("binlog_format : "+binlogFormat);
                    bw.newLine();
                    if(!"ROW".equals(binlogFormat.trim())){
                        bw.write("检查失败，请确认bin format及同步模式..");
                        bw.newLine();
                        throw new Exception();
                    }
                }

                //检查主备库同步信息，如果能查到就是1行。
               /* bw.write("检查备库同步信息: "+ slaveStatus+"...");
                bw.newLine();
                try {
                    rs = stm.executeQuery(slaveStatus);
                    if (rs.next()) {
                        bw.write("Master_Log_File : " + rs.getString("Master_Log_File"));
                        bw.newLine();
                        bw.write("Read_Master_Log_Pos : " + rs.getString("Read_Master_Log_Pos"));
                        bw.newLine();
                        bw.write("Relay_Log_File : " + rs.getString("Relay_Log_File"));
                        bw.newLine();
                        bw.write("Relay_Log_Pos : " + rs.getString("Relay_Log_Pos"));
                        bw.newLine();
                        bw.write("Relay_Master_Log_File : " + rs.getString("Relay_Master_Log_File"));
                        bw.newLine();

                    } else {
                        bw.write("备库同步信息异常，请确认备库设置！");
                        bw.newLine();
                        throw new Exception();
                    }
                }catch (Exception e){
                    bw.write("执行异常，请确认备库权限等设置！");
                    bw.newLine();
                    throw e;
                }*/


                //检查主库信息，如果能查到就是1行。 检查出异常后正常执行
                /*try {
                    bw.write("检查主库信息: " + materStatus + "...");
                    bw.newLine();
                    rs = stm.executeQuery(materStatus);
                    if (rs.next()) {
                        bw.write("Position : " + rs.getString("Position"));
                        bw.newLine();
                    } else {
                        bw.write("执行结果异常");
                        bw.newLine();
                        throw new Exception();
                    }
                }catch (Exception e){
                    bw.write("执行异常，请确认备库权限等设置！");
                    bw.newLine();
                    throw e;
                }*/

                rs.close();
                stm.close();
                bw.write("-----check database canal account success");
                bw.newLine();
                closeAll(conn, null, null, bw);
            } else {
                bw.write("数据库检查连接失败，请检查canal用户名、密码以及源端库备库地址！");
                bw.newLine();
                closeAll(conn, null, null, bw);
                throw new Exception();
            }
        }catch (Exception e){
            bw.write("数据库检查失败，请确认以下配置");
            bw.newLine();
            bw.write("1.canal用户名、密码以及源端库备库地址。2.canal用户的授权。3.源端库主库和备库binlog相关设置");
            bw.newLine();
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
