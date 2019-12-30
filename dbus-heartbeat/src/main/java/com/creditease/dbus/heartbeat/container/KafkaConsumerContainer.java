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


package com.creditease.dbus.heartbeat.container;

import com.creditease.dbus.heartbeat.event.impl.KafkaConsumerEvent;
import com.creditease.dbus.heartbeat.log.LoggerFactory;
import org.apache.kafka.clients.consumer.Consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KafkaConsumerContainer {

    private static KafkaConsumerContainer container;

    private ExecutorService es;

    private ConcurrentHashMap<String, Consumer<String, String>> consumerMap = new ConcurrentHashMap<String, Consumer<String, String>>();

    private List<KafkaConsumerEvent> kafkaConsumerEvent = Collections.synchronizedList(new ArrayList<KafkaConsumerEvent>());

    private KafkaConsumerContainer() {
    }

    public static KafkaConsumerContainer getInstances() {
        if (container == null) {
            synchronized (KafkaConsumerContainer.class) {
                if (container == null)
                    container = new KafkaConsumerContainer();
            }
        }
        return container;
    }

    public void putConsumer(String key, Consumer<String, String> consumer) {
        consumerMap.put(key, consumer);
    }

    public Consumer<String, String> getConsumer(String key) {
        return consumerMap.get(key);
    }

    public void initThreadPool(int size) {
        int poolSize = size + 10;
        es = Executors.newFixedThreadPool(poolSize);
        LoggerFactory.getLogger().info("[kafka-consumer-container] initThreadPool size = " + poolSize);
    }

    public void submit(KafkaConsumerEvent event) {
        kafkaConsumerEvent.add(event);
        es.submit(event);
    }

    public void shutdown() {
        try {
            //发起礼貌退出通知
            for (KafkaConsumerEvent event : kafkaConsumerEvent) {
                event.stop();
            }

            //发起shutdown线程池, 正在执行的thread不强停
            this.es.shutdown();

            //等5秒看是否礼貌退出已经关闭
            if (!this.es.awaitTermination(5L, TimeUnit.SECONDS)) {

                //强制关闭包括正在执行的线程
                this.es.shutdownNow();

                for (int i = 1; i <= 5; i++) {
                    sleep(1L, TimeUnit.SECONDS);
                    if (this.es.isTerminated()) {
                        LoggerFactory.getLogger().info("[kafka-consumer-container] thread pool interrupt shutdown success!!");
                        break;
                    }
                    LoggerFactory.getLogger().info("[kafka-consumer-container] thread pool shutdown, retry time: {}", i);
                }
                //如果重复等待5次后仍然无法退出，就system exit.
                if (!this.es.isTerminated()) {
                    LoggerFactory.getLogger().info("[kafka-consumer-container] force exit!!!");
                    sleep(1L, TimeUnit.SECONDS);
                    System.exit(-1);
                }
            } else {
                LoggerFactory.getLogger().info("[kafka-consumer-container] thread pool normal shutdown success!!");
            }
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[kafka-consumer-container]", e);
            sleep(1L, TimeUnit.SECONDS);
            System.exit(-1);
        } finally {
            es = null;
        }
        consumerMap.clear();
        kafkaConsumerEvent.clear();
        container = null;
    }

    private void sleep(long t, TimeUnit tu) {
        try {
            tu.sleep(t);
        } catch (InterruptedException e) {
            LoggerFactory.getLogger().error("[kafka-consumer-container] 线程sleep:" + t + " " + tu.name() + "中被中断!");
        }
    }
}
