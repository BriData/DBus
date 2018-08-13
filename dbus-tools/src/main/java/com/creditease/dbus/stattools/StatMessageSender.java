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

package com.creditease.dbus.stattools;



import com.creditease.dbus.commons.StatMessage;
import com.creditease.dbus.tools.common.AbstractLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.util.List;
import java.util.concurrent.ThreadFactory;

/**
 * Created by dongwang47 on 2016/9/2.
 */
@Deprecated
public class StatMessageSender extends AbstractLauncher implements SignalHandler {
    private static Logger logger = LoggerFactory.getLogger(StatMessageSender.class);

    //外部退出条件和标志
    private volatile boolean running = true;
    KafkaSource source = null;
    InfluxSink sink = null;

    public void run() {
        try {
            source = new KafkaSource();
            sink = new InfluxSink();

            List<StatMessage> list = null;
            long retryTimes = 0;
            while (running) {
                list = source.poll();
                if (list == null)
                    continue;

                retryTimes = 0;
                while (running) {
                    if (sink.sendBatchMessages(list, retryTimes) == 0) {
                        source.commitOffset();
                        //如果写influxdb成功， 就退出循环
                        break;
                    }

                    //写influxdb失败，等待一会，继续重试
                    retryTimes++;
                    Thread.sleep(5000);
                }
            }
        } catch (Exception ex) {
            logger.info(ex.getMessage());
            ex.printStackTrace();
        } finally {
            if (source != null) {
                source.cleanUp();
                source = null;
            }

            if (sink != null) {
                sink.cleanUp();
                sink = null;
            }
        }
    }

    public static void main(String[] args) {
        StatMessageSender sender = new StatMessageSender();

        // kill命令
        Signal termSignal = new Signal("TERM");
        Signal.handle(termSignal, sender);
        // ctrl+c命令
        Signal intSignal = new Signal("INT");
        Signal.handle(intSignal, sender);

        sender.run();
    }

    @Override
    public void handle(Signal signal) {
        System.out.println("Signal handler called for signal " + signal);
        try {
            System.out.println("politely exiting, please wait 3 seconds...");
            running = false;
        } catch (Exception e) {
            System.out.println("handle|Signal handler" + "failed, reason "
                    + e.getMessage());
            e.printStackTrace();
        }
    }
}
