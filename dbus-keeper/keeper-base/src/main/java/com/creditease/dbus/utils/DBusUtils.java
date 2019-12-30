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


package com.creditease.dbus.utils;

import com.google.common.hash.Hashing;
import org.apache.commons.beanutils.BeanMap;
import org.apache.commons.beanutils.BeanUtils;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhangyf on 2018/3/19.
 */
public class DBusUtils {
    public static String md5(String oriStr) {
        return md5(oriStr, "");
    }

    public static String md5(String oriStr, String salt) {
        return Hashing.md5()
                .hashString(oriStr + salt, Charset.forName("utf-8"))
                .toString();
    }

    public static <T> T map2bean(Map<String, Object> map, Class<T> clazz) {
        try {
            T t = clazz.newInstance();
            BeanUtils.populate(t, map);
            return t;
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static Map<String, Object> object2map(Object obj) {
        if (obj == null)
            return null;
        Map<?, ?> map = new BeanMap(obj);
        Map<String, Object> resultMap = new HashMap<>(map.size());
        for (Object key : map.keySet()) {
            resultMap.put(key.toString(), map.get(key));
        }
        return resultMap;
    }

    /**
     * 将驼峰格式的输入转换为下划线方式
     *
     * @param input
     * @return
     */
    public static String underscoresNaming(String input) {
        if (input == null) {
            return input;
        } else {
            int length = input.length();
            StringBuilder result = new StringBuilder(length * 2);
            int resultLength = 0;
            boolean wasPrevTranslated = false;

            for (int i = 0; i < length; ++i) {
                char c = input.charAt(i);
                if (i > 0 || c != 95) {
                    if (Character.isUpperCase(c)) {
                        if (!wasPrevTranslated && resultLength > 0 && result.charAt(resultLength - 1) != 95) {
                            result.append('_');
                            ++resultLength;
                        }

                        c = Character.toLowerCase(c);
                        wasPrevTranslated = true;
                    } else {
                        wasPrevTranslated = false;
                    }

                    result.append(c);
                    ++resultLength;
                }
            }
            return resultLength > 0 ? result.toString() : input;
        }
    }
}
