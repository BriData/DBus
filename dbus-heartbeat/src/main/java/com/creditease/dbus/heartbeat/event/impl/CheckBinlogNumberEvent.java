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


package com.creditease.dbus.heartbeat.event.impl;

import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.dao.IHeartBeatDao;
import com.creditease.dbus.heartbeat.dao.impl.HeartBeatDaoImpl;
import com.creditease.dbus.heartbeat.event.AbstractEvent;
import com.creditease.dbus.heartbeat.util.Constants;
import com.creditease.dbus.heartbeat.util.DateUtil;
import com.creditease.dbus.heartbeat.util.MsgUtil;
import com.creditease.dbus.heartbeat.vo.DsVo;
import com.creditease.dbus.heartbeat.vo.HeartBeatVo;
import com.creditease.dbus.heartbeat.vo.MysqlMasterStatusVo;
import com.creditease.dbus.mail.DBusMailFactory;
import com.creditease.dbus.mail.IMail;
import com.creditease.dbus.mail.Message;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class CheckBinlogNumberEvent extends AbstractEvent {

    private Logger logger = LoggerFactory.getLogger(CheckBinlogNumberEvent.class);

    private IHeartBeatDao dao;

    private Map<String, Integer> cache;

    public CheckBinlogNumberEvent(long interval) {
        super(interval);
        dao = new HeartBeatDaoImpl();
        cache = new HashMap<>();
    }

    @Override
    public void run() {
        List<DsVo> dsVos = HeartBeatConfigContainer.getInstance().getDsVos();
        LOG.info("check-mysql-binlog-event stare");
        while (isRun.get()) {
            for (DsVo ds : dsVos) {
                try {
                    if (!isRun.get())
                        break;

                    if (DbusDatasourceType.stringEqual(ds.getType(), DbusDatasourceType.MYSQL)) {
                        MysqlMasterStatusVo statusVo = dao.queryMasterStatus(ds.getKey());
                        if (statusVo != null) {
                            Integer fileNum = NumberUtils.toInt(StringUtils.split(statusVo.getFile(), ".")[1]);
                            logger.info("数据源:{},当前binlog文件号:{}", ds.getKey(), fileNum);

                            if (cache.containsKey(ds.getKey())) {
                                if (cache.get(ds.getKey()) > fileNum) {
                                    // 报警
                                    IMail mail = DBusMailFactory.build();
                                    String subject = "DBus binlog报警 ";
                                    String contents = MsgUtil.format(Constants.MAIL_MYSQL_MASTER_STATUS,
                                            "binlog报警", ds.getKey(),
                                            DateUtil.convertLongToStr4Date(System.currentTimeMillis()),
                                            IMail.ENV);

                                    // 获取心跳配置信息
                                    HeartBeatVo hbConf = HeartBeatConfigContainer.getInstance().getHbConf();

                                    String adminUseEmail = hbConf.getAdminUseEmail();
                                    String adminEmail = hbConf.getAdminEmail();
                                    String email = StringUtils.EMPTY;
                                    if (StringUtils.equals(adminUseEmail.toUpperCase(), "Y")) {
                                        email = adminEmail;
                                    }
                                    if (StringUtils.isNotBlank(email)) {
                                        Message msg = new Message();
                                        msg.setAddress(email);
                                        msg.setContents(contents);
                                        msg.setSubject(subject);

                                        msg.setHost(hbConf.getAlarmMailSMTPAddress());
                                        if (StringUtils.isNotBlank(hbConf.getAlarmMailSMTPPort()))
                                            msg.setPort(Integer.valueOf(hbConf.getAlarmMailSMTPPort()));
                                        msg.setUserName(hbConf.getAlarmMailUser());
                                        msg.setPassword(hbConf.getAlarmMailPass());
                                        msg.setFromAddress(hbConf.getAlarmSendEmail());

                                        mail.send(msg);
                                    } else {
                                        logger.warn("mysql binlog文件号变小,邮箱地址为空,不能发出报警,报警信息:{}", contents);
                                    }
                                } else if (cache.get(ds.getKey()) < fileNum) {
                                    cache.put(ds.getKey(), fileNum);
                                }
                            } else {
                                cache.put(ds.getKey(), fileNum);
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.error("check binlog error,ds name: {}", ds.getKey());
                }
            }
            sleep(interval, TimeUnit.SECONDS);
        }
        LOG.info("check-mysql-binlog-event stop");
    }

}
