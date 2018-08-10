/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2018 Bridata
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

package com.creditease.dbus;

import com.alibaba.fastjson.JSON;
import com.creditease.dbus.encoders.EncoderConf;
import com.creditease.dbus.encoders.ExtEncodeStrategy;
import com.creditease.dbus.plugins.RegexStrategy;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhangyf on 2018/5/29.
 */
public class RegexStrategyTest {
    private static EncoderConf conf;

    @BeforeClass
    public static void setup() throws Exception {
        conf = new EncoderConf();
        Map<String, String> map = new HashMap<>();

        File file = new File("D:\\bridata\\dbus\\dbus-main\\encoder-plugins\\src\\test\\java\\com\\creditease\\dbus\\json.text");
       String param = FileUtils.readFileToString(file, "utf-8");

        map.put("regex", "(\\d{3})(\\d{4})(\\d{4})");
        map.put("replacement", "$1*******");
        conf.setEncodeParam(JSON.toJSONString(map));
        conf.setEncodeParam(param);
    }

    @Test
    public void test() {
        ExtEncodeStrategy s = new RegexStrategy();
        Assert.assertEquals("", s.encode(conf, "+86-13800138000"));

    }
}
