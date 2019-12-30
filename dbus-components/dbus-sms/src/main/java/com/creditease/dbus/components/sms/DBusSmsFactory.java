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


package com.creditease.dbus.components.sms;

import java.io.InputStream;
import java.util.Properties;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Administrator on 2017/8/10.
 */
public class DBusSmsFactory {

    private static Logger LOG = LoggerFactory.getLogger(DBusSmsFactory.class);

    private static String INSTANCE_CLAZZ;

    private static ISms sms = null;

    static {
        InputStream is = null;
        try {
            is = DBusSmsFactory.class.getClassLoader().getResourceAsStream("sms.properties");
            Properties mailProps = new Properties();
            mailProps.load(is);
            INSTANCE_CLAZZ = mailProps.getProperty("instance.clazz");
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

    private DBusSmsFactory() {
    }

    public static ISms bulid() {
        if (sms == null) {
            synchronized (DBusSmsFactory.class) {
                if (sms == null)
                    try {
                        sms = (ISms) Class.forName(INSTANCE_CLAZZ).newInstance();
                    } catch (Exception e) {
                        LOG.error(e.getMessage(), e);
                        throw new RuntimeException(e);
                    }
            }
        }
        return sms;
    }

}
