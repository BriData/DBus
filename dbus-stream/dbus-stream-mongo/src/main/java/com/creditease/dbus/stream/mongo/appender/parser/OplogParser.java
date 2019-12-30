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


package com.creditease.dbus.stream.mongo.appender.parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ximeiwang on 2017/12/21.
 */
public class OplogParser {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private static OplogParser parser;

    private OplogParser() {
    }

    public static OplogParser getInstance() {
        if (parser == null) {
            synchronized (OplogParser.class) {
                if (parser == null) {
                    parser = new OplogParser();
                }
            }
        }
        return parser;
    }

    public String getEntry(byte[] input) throws Exception {
        //TODO 解析
        //List<String> list = new ArrayList<>();
        String entry = new String(input);
        return entry;
    }
}
