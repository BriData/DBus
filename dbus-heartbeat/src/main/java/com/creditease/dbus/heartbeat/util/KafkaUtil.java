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


package com.creditease.dbus.heartbeat.util;

import com.creditease.dbus.heartbeat.container.CuratorContainer;
import com.creditease.dbus.heartbeat.container.HeartBeatConfigContainer;
import com.creditease.dbus.heartbeat.log.LoggerFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.ByteArrayInputStream;
import java.util.Properties;

import static com.creditease.dbus.commons.Constants.COMMON_ROOT;
import static com.creditease.dbus.commons.Constants.GLOBAL_SECURITY_CONF;
import static com.creditease.dbus.heartbeat.util.Constants.SECURITY_CONFIG_KEY;
import static com.creditease.dbus.heartbeat.util.Constants.SECURITY_CONFIG_TRUE_VALUE;

public class KafkaUtil {

    private KafkaUtil() {
    }

    public static boolean send(String msg, String topic, final String key) {
        boolean isOk = true;
        KafkaProducer<String, String> producer = null;
        try {
            Properties props = HeartBeatConfigContainer.getInstance().getKafkaProducerConfig();
            //安全
            if (checkSecurity()) {
                props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            }
            producer = new KafkaProducer<String, String>(props);
            producer.send(new ProducerRecord<String, String>(topic, key, msg), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null)
                        LoggerFactory.getLogger().error("[kafka-send-error]", e);
                    else {
                        LoggerFactory.getLogger().info(
                                String.format("信息发送到topic:%s,key:%s,offset:%s",
                                        metadata.topic(), key, metadata.offset()));
                    }

                }
            });
        } catch (Exception e) {
            isOk = false;
            LoggerFactory.getLogger().error("[kafka-send-error]", e);
        } finally {
            if (producer != null)
                producer.close();
        }
        return isOk;
    }

    /**
     * 判断是否开启了安全模式
     *
     * @return true 开启； false: 没有开启，或出错
     */
    public static boolean checkSecurity() {
        try {
            CuratorFramework curator = CuratorContainer.getInstance().getCurator();
            String path = COMMON_ROOT + "/" + GLOBAL_SECURITY_CONF;
            if (curator.checkExists().forPath(path) != null) {
                byte[] data = curator.getData().forPath(path);
                if (data == null || data.length == 0) {
                    LoggerFactory.getLogger().error("[checkSecurity] 加载zk path: " + path + "配置信息不存在.");
                    return false;
                }
                Properties properties = new Properties();
                properties.load(new ByteArrayInputStream(data));

                String securityConf = properties.getProperty(SECURITY_CONFIG_KEY);
                if (StringUtils.equals(securityConf, SECURITY_CONFIG_TRUE_VALUE)) {
                    return true;
                } else {
                    return false;
                }
            } else {
                LoggerFactory.getLogger().error("[checkSecurity]zk path :" + path + "不存在");
                return false;
            }
        } catch (Exception e) {
            LoggerFactory.getLogger().error("[checkSecurity]check error:  加载zk node 出错 path: " +
                    COMMON_ROOT + "/" + GLOBAL_SECURITY_CONF);
            return false;
        }

    }

}
