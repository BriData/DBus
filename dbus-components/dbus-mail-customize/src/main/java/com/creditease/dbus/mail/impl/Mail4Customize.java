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

package com.creditease.dbus.mail.impl;

import java.io.InputStream;
import java.util.Properties;

import com.creditease.dbus.mail.IMail;
import com.creditease.dbus.mail.Message;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Administrator on 2017/8/15.
 */
public class Mail4Customize extends IMail {

    private static Logger LOG = LoggerFactory.getLogger(Mail4Customize.class);

    static {
        InputStream is = null;
        try {
            is = Mail4Customize.class.getClassLoader().getResourceAsStream("mail.properties");
            Properties mailProps = new Properties();
            mailProps.load(is);
            ENV = mailProps.getProperty("env");
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

    @Override
    public boolean send(Message msg) {
        // TODO 利用标准 java Mail api 发送邮件暂时未实现
        LOG.info(String.format("Address: %s", msg.getAddress()));
        LOG.info(String.format("Subject: %s", msg.getContents() + ENV));
        LOG.info(String.format("Contents: %s", msg.getContents()));
        return true;
    }

}
