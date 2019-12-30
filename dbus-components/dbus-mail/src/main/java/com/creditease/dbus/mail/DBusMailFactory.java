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


package com.creditease.dbus.mail;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Administrator on 2017/8/10.
 */
public class DBusMailFactory {

    private static Logger LOG = LoggerFactory.getLogger(DBusMailFactory.class);

    private static String INSTANCE_CLAZZ;

    private static IMail mail = null;

    static {
        InputStream is = null;
        try {
            is = DBusMailFactory.class.getClassLoader().getResourceAsStream("mail.properties");
            Properties mailProps = new Properties();
            mailProps.load(is);
            INSTANCE_CLAZZ = mailProps.getProperty("instance.clazz");
            LOG.info("dbus mail factory load instance clazz:{}", INSTANCE_CLAZZ);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

    private DBusMailFactory() {
    }

    public static IMail build() {
        if (mail == null) {
            synchronized (DBusMailFactory.class) {
                if (mail == null)
                    try {
                        if (INSTANCE_CLAZZ == null || "".equals(INSTANCE_CLAZZ))
                            throw new IllegalArgumentException("instance class is empty.");
                        LOG.info("build mail instance start.");
                        mail = (IMail) Class.forName(INSTANCE_CLAZZ).newInstance();
                        LOG.info("build mail instance end.");
                    } catch (Throwable e) {
                        LOG.error(e.getMessage(), e);
                        throw new RuntimeException(e);
                    }
            }
        }
        return mail;
    }

}
