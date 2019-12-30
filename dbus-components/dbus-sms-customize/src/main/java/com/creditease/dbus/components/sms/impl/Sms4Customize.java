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


package com.creditease.dbus.components.sms.impl;

import com.creditease.dbus.components.sms.ISms;
import com.creditease.dbus.components.sms.SmsMessage;
import java.io.InputStream;
import java.util.Properties;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Administrator on 2017/8/16.
 */
public class Sms4Customize  implements ISms{

    private static Logger LOG = LoggerFactory.getLogger(Sms4Customize.class);

    private static String ENV;

    static {
        InputStream is = null;
        try {
            is = Sms4Customize.class.getClassLoader().getResourceAsStream("sms.properties");
            Properties smsProps = new Properties();
            smsProps.load(is);
            ENV = smsProps.getProperty("env");
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

    @Override
    public boolean send(SmsMessage msg) {
        // TODO 发送短信功能暂时未实现，请客户根据公司实际情况实现
        LOG.info(String.format("telNo: %s", msg.getTelNo()));
        LOG.info(String.format("Contents: %s", msg.getContents()));
        return true;
    }

}
