/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2017 Bridata
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

package com.creditease.dbus.ws;

import com.creditease.dbus.mgr.base.ConfUtils;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

/**
 * mybatis SqlSession获取工具
 * Created by dongwang47 on 2016/8/31.
 */
public class SessionUtil {

    private static SqlSessionFactory sessionFactory = null;

    static {
        //ConfUtils.mybatisConf
        File file = new File(ConfUtils.CONF_DIR + File.separator + "mybatis-config.xml");
        Reader reader = null;
        try {
            reader = new FileReader(file);
            sessionFactory = new SqlSessionFactoryBuilder().build(reader, ConfUtils.mybatisConf);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (reader != null) reader.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    //Mybatis 通过SqlSessionFactory获取SqlSession, 然后才能通过SqlSession与数据库进行交互
    public static SqlSessionFactory getSessionFactory() {
        return sessionFactory;
    }

    public static SqlSession openSession() {
        return sessionFactory.openSession();
    }
}
