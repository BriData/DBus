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

import com.creditease.dbus.commons.StatMessage;
import com.creditease.dbus.heartbeat.event.AbstractEvent;
import com.creditease.dbus.heartbeat.stattools.InfluxSink;
import com.creditease.dbus.heartbeat.stattools.KafkaSource;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class SendStatMessageEvent extends AbstractEvent {

    private KafkaSource source = null;
    private InfluxSink sink = null;

    public SendStatMessageEvent(long interval) {
        super(interval);
    }

    @Override
    public void run() {
        try {
            source = new KafkaSource();
            sink = new InfluxSink();
            List<StatMessage> list = null;
            long retryTimes = 0;
            while (isRun.get()) {
                list = source.poll();
                if (list == null)
                    continue;

                retryTimes = 0;
                while (isRun.get()) {
                    if (sink.sendBatchMessages(list, retryTimes) == 0) {
                        source.commitOffset();
                        //如果写influxdb成功， 就退出循环
                        break;
                    }
                    //写influxdb失败，等待一会，继续重试
                    retryTimes++;
                    sleep(interval, TimeUnit.SECONDS);
                }
            }
        } catch (Exception e) {
            LOG.error("[send-stat-msg-event]", e);
        } finally {
            if (source != null) {
                source.cleanUp();
                source = null;
            }
            if (sink != null) {
                sink.cleanUp();
                sink = null;
            }
            LOG.info("[send-stat-msg-event] exit.");
        }
    }
}
