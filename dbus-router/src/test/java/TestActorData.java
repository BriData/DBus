/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2018 Bridata
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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.creditease.dbus.router.container.DataSourceContainer;
import com.creditease.dbus.router.facade.ZKFacade;
import com.creditease.dbus.router.util.DBUtil;
import com.creditease.dbus.router.util.DBusRouterConstants;

/**
 * Created by Administrator on 2018/6/12.
 */
public class TestActorData {

    public static void main(String[] args) throws Exception {
        ZKFacade zkHelper = new ZKFacade("vdbus-7:2181", "tr", "");
        Properties props = zkHelper.loadMySqlConf();
        props.setProperty("url", "jdbc:mysql://vdbus-10:3310/test?characterEncoding=utf-8");
        props.setProperty("username", "root");
        props.setProperty("password", "HULyDjLaZZxR0TuV");
        DataSourceContainer.getInstance().register(DBusRouterConstants.DBUS_ROUTER_DB_KEY, props);
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = DataSourceContainer.getInstance().getConn(DBusRouterConstants.DBUS_ROUTER_DB_KEY);
            StringBuilder sql = new StringBuilder();
            sql.append("insert into actor (first_name, last_name, col_2, col_1) values (?,?,?,?)");
            ps = conn.prepareStatement(sql.toString());
            long s = System.currentTimeMillis();

            while (System.currentTimeMillis() - s <= (1000 * 60 * 10)) {
                for (int i=0; i<10; i++) {
                    int idx = 1;
                    ps.setString(idx++, "test_first_name_" + i);
                    ps.setString(idx++, "test_last_name_" + i);
                    ps.setString(idx++, "" + i);
                    /*ps.setString(idx++, "" + i);
                    ps.setString(idx++, "" + i);*/
//                    ps.setString(idx++, "" + i);
                    ps.setString(idx++, "" + i);
                    ps.execute();
                    System.out.println(i);
                }
                TimeUnit.SECONDS.sleep(5);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBUtil.close(rs);
            DBUtil.close(ps);
            DBUtil.close(conn);
            DataSourceContainer.getInstance().clear();
        }

    }

}
