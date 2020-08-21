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


package com.creditease.dbus.mail.impl;

import com.creditease.dbus.mail.IMail;
import com.creditease.dbus.mail.Message;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

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
        boolean isOk = true;
        Transport transport = null;
        try {
            LOG.info(String.format("Address: %s", msg.getAddress()));
            LOG.info(String.format("Subject: %s", msg.getSubject() + ENV));
            LOG.info(String.format("Contents: %s", msg.getContents()));
            LOG.info(String.format("Host: %s", msg.getHost()));
            LOG.info(String.format("Port: %s", msg.getPort()));
            LOG.info(String.format("User name: %s", msg.getUserName()));

            Properties props = new Properties();
            props.setProperty("mail.smtp.auth", "true");
            props.setProperty("mail.transport.protocol", "smtp");
            props.setProperty("mail.smtp.host", msg.getHost());

            Session session = Session.getInstance(props);
            // session.setDebug(true);

            MimeMessage message = new MimeMessage(session);
            message.setFrom(msg.getFromAddress());
            message.setRecipients(javax.mail.Message.RecipientType.TO, msg.getAddress());
            message.setSubject(msg.getSubject());
            //指定邮件优先级 1：紧急 3：普通 5：缓慢
            String priority = msg.getPriority() == null ? "3" : msg.getPriority();
            message.setHeader("X-Priority", priority);

            MimeMultipart mmp = new MimeMultipart();
            MimeBodyPart mbp = new MimeBodyPart();
            mbp.setContent(msg.getContents(), "text/html;charset=UTF-8");

            mmp.addBodyPart(mbp);
            mmp.setSubType("mixed");

            message.setContent(mmp);
            message.setSentDate(new Date());

            transport = session.getTransport();
            transport.connect(msg.getHost(), msg.getPort(), msg.getUserName(), msg.getPassword());
            transport.sendMessage(message, message.getAllRecipients());

            LOG.error("mail for customize send email success.");
        } catch (Exception e) {
            isOk = false;
            LOG.error("mail for customize send email fail.", e);
        } finally {
            close(transport);
        }
        return isOk;
    }

    private void close(Transport transport) {
        if (transport != null)
            try {
                transport.close();
            } catch (MessagingException e) {
                LOG.error("close java mail transport fail.", e);
            }
    }

}
