package com.creditease.dbus.allinone.auto.check.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Administrator on 2018/8/1.
 */
public class DBUtils {

    private static Logger logger = LoggerFactory.getLogger(DBUtils.class);

    private DBUtils() {
    }

    public static Connection getConn(String url, String name, String password) throws Exception {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url, name, password);
        } catch (ClassNotFoundException e) {
            logger.error("DBUtils getConn method error.", e);
            throw e;
        } catch (SQLException e) {
            logger.error("DBUtils getConn method error.", e);
            throw e;
        }
        return conn;
    }

    public static void close(Object obj) {
        if (obj == null) return;
        try {
            if (obj instanceof PreparedStatement) ((PreparedStatement) obj).close();
            if (obj instanceof ResultSet) ((ResultSet) obj).close();
            if (obj instanceof Connection) ((Connection) obj).close();
        } catch (SQLException e) {
            logger.error("DBUtils close method error.", e);
        }
    }

}
