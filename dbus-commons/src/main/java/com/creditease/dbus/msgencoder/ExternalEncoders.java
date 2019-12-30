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


package com.creditease.dbus.msgencoder;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.DistributedLock;
import com.creditease.dbus.commons.ZkService;
import com.creditease.dbus.encoders.Encoder;
import com.creditease.dbus.encoders.ExtEncodeStrategy;
import com.creditease.dbus.enums.MessageEncodeType;
import com.creditease.dbus.utils.AnnotationScanner;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;


/**
 * Created by zhangyf on 17/5/31.
 */
@Deprecated
public class ExternalEncoders {
    private static Logger logger = LoggerFactory.getLogger(ExternalEncoders.class);
    private static final Map<String, Class<ExtEncodeStrategy>> map = new HashMap<>();
    //private static final Map<String, Class<ExtEncodeStrategy>> map2 = new HashMap<>();
    private static ZkService zkService;

//    static {
//        map.putAll(scan());
//        logger.info("external message encoders:{}", map);
//        System.out.println(map);
//    }

    public static Map<String, Class<ExtEncodeStrategy>> get() {
        return map;
    }

    public static void setMap() {
        map.putAll(scan());
    }

    public void initExternalEncoders(String zkConnect, ZkService zkService) {
        this.zkService = zkService;
        //保证只有一个线程可以扫描classpath，其他线程通过读取zookeeper获取第三方加密类型。
        DistributedLock.distributedLock(Constants.ENCODER_PLUGINS, zkConnect);
    }

    //保证只有一个线程可以扫描classpath，并且将脱敏类型(包括内置类型)写入zookeeper
    public static void saveEncoderType(String path) {
        //内置的脱敏类型
        StringBuilder builtInEncry = new StringBuilder("BuiltInEncodeType=");
        for (MessageEncodeType type : MessageEncodeType.values()) {
            builtInEncry.append(type.name().toLowerCase() + ",");
        }
        int lastIndex = builtInEncry.lastIndexOf(",");
        String builtInEncryStr = builtInEncry.substring(0, lastIndex) + "\n";

        //扫描classpath，检测第三方加密类型
        ExternalEncoders.setMap();
        StringBuilder external = new StringBuilder();
        Map<String, Class<ExtEncodeStrategy>> extMap = ExternalEncoders.get();
        for (Map.Entry<String, Class<ExtEncodeStrategy>> entry : extMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue().getName();
            external.append(value + "=" + key + "\n");
        }

        String encoderTypeStr = builtInEncryStr + external.toString();
        //写zookeeper
        try {
            if (zkService.isExists(path)) {
                zkService.setData(path, encoderTypeStr.getBytes());
            } else {
                zkService.createNode(path, encoderTypeStr.getBytes());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //其他线程从zk节点中读取脱敏类型，将第三方脱敏类型写入map
    public static void getEncoderTypeFromZk(String path) {
        try {
            if (zkService.isExists(path)) {
                byte[] encoderTypes = zkService.getData(path);
                Properties properties = new Properties();
                InputStream is = new ByteArrayInputStream(encoderTypes);
                properties.load(is);
                String type;
                for (String key : properties.stringPropertyNames()) {
                    //对于第三方脱敏类型，利用反射机制获取class，并将其加入map中
                    type = properties.getProperty(key);
                    if (!StringUtils.equals(key, "BuiltInEncodeType")) {
                        Class<?> clazz = Class.forName(key);
                        map.put(type, (Class<ExtEncodeStrategy>) clazz);
                    }
                    logger.info("key: " + key + "  value: " + type);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static Map<String, Class<ExtEncodeStrategy>> scan() {
        Map<String, Class<ExtEncodeStrategy>> map = new HashMap<>();
        Set<Class<?>> classSet = AnnotationScanner.scan(Encoder.class);
        for (Class<?> clazz : classSet) {
            Encoder encoder = clazz.getAnnotation(Encoder.class);
            assert encoder != null;
            String type = encoder.type().toLowerCase();
            if (map.containsKey(type)) {
                throw new RuntimeException("encoder type[" + encoder.type() + "] exists.");
            }
            map.put(type, (Class<ExtEncodeStrategy>) clazz);
        }
        return map;
    }


    public static void main(String[] args) {
        ExternalEncoders.get();
    }
}
