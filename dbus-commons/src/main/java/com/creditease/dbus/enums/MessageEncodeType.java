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


package com.creditease.dbus.enums;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhangyf on 16/11/10.
 */
public enum MessageEncodeType {
    DEFAULT,
    REPLACE,
    HASH_MD5,
    HASH_MURMUR,
    HASH_MD5_FIELD_SALT, // 以message中某字段值为salt的md5算法
    HASH_MD5_FIXED_SALT, // 固定salt值的md5算法
    //ADDRESS_NORMALIZE, // 结果事例：广东省广州市花都区建设路旧食街７号耐克专卖店广州美鞋子体 => 广东省广州市花都区建设路旧食街７号
    REGEX_NORMALIZE;
    //YISOU_DATA_CLEAN;//姨搜数据预处理(导包)
    private static Map<String, String> map;

    static {
        map = new HashMap<>();
        for (MessageEncodeType type : values()) {
            map.put(type.name().toLowerCase(), type.name());
        }
    }

    public static Map<String, String> getMap() {
        return map;
    }

    public static void main(String[] args) {

        System.out.println("");
    }
}
