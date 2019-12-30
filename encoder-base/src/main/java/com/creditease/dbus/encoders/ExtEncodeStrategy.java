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


package com.creditease.dbus.encoders;

/**
 * 外部数据预处理/脱敏策略接口
 * 实现该接口需要配合使用com.creditease.dbus.encoders.Encoder注解标记
 * Created by zhangyf on 16/11/10.
 */
public interface ExtEncodeStrategy {
    /**
     * 预处理/脱敏实现方法
     *
     * @param conf  脱敏配置，dbus处理数据时会将数据库中的脱敏配置信息以及ums相关信息
     *              转换为EncoderConf对象传递给具体脱敏策略
     * @param value 预处理/脱敏前的原始字段值
     * @return 预处理/脱敏后的内容
     */
    Object encode(EncoderConf conf, Object value);

    default void addResource(String resourceName, byte[] data) {
    }

    default byte[] getResource(String resourceName) {
        return null;
    }
}
