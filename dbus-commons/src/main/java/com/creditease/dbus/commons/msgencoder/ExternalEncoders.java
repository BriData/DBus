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

package com.creditease.dbus.commons.msgencoder;

import com.creditease.dbus.encoders.Encoder;
import com.creditease.dbus.encoders.ExtEncodeStrategy;
import com.creditease.dbus.utils.AnnotationScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by zhangyf on 17/5/31.
 */
public class ExternalEncoders {
    private static Logger logger = LoggerFactory.getLogger(ExternalEncoders.class);
    private static final Map<String, Class<ExtEncodeStrategy>> map = new HashMap<>();

    static {
        map.putAll(scan());
        logger.info("external message encoders:{}", map);
        System.out.println(map);
    }

    public static Map<String, Class<ExtEncodeStrategy>> get() {
        return map;
    }

    private static Map<String, Class<ExtEncodeStrategy>> scan() {
        Map<String, Class<ExtEncodeStrategy>> map = new HashMap<>();
        Set<Class<?>> classSet = AnnotationScanner.scan(Encoder.class);
        for (Class<?> clazz : classSet) {
            Encoder encoder = clazz.getAnnotation(Encoder.class);
            assert encoder != null;
            String type = encoder.type().toLowerCase();
            if (map.containsKey(type)) {
                throw new RuntimeException("encoder type[" + encoder.type() + "] exists.");
            }
            map.put(type, (Class<ExtEncodeStrategy>)clazz);
        }
        return map;
    }

    public static void main(String[] args) {
        ExternalEncoders.get();
    }
}
