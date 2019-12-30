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


package com.creditease.dbus.plugins;

import com.creditease.dbus.encoders.Encoder;
import com.creditease.dbus.encoders.EncoderConf;
import com.google.common.hash.Hashing;

/**
 * 配置信息:
 * 固定盐值：{"type":"fixed", "salt": "xxxxxx"}
 * 以数据中某一字段做盐值：{"type":"field", "salt": "user_name"}
 */
@Encoder(type = "md5")
public class Md5HashStrategy extends HashStrategy {
    @Override
    protected HashCode hash(Object str, EncoderConf conf) {
        return new DBusHashCode(Hashing.md5().hashString(str.toString() + getSalt(conf), UTF8));
    }
}
