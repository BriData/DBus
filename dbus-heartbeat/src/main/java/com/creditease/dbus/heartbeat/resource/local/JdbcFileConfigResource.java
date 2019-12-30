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


package com.creditease.dbus.heartbeat.resource.local;

import com.creditease.dbus.heartbeat.vo.JdbcVo;

import java.util.ArrayList;
import java.util.List;

public class JdbcFileConfigResource extends FileConfigResource<List<JdbcVo>> {

    public JdbcFileConfigResource(String name) {
        super(name);
    }

    @Override
    public List<JdbcVo> parse() {
        List<JdbcVo> confs = new ArrayList<JdbcVo>();
        try {
            JdbcVo conf = new JdbcVo();
            conf.setDriverClass(prop.getProperty("DB_DRIVER_CLASS"));
            conf.setUrl(prop.getProperty("DB_URL"));
            conf.setUserName(prop.getProperty("DB_USER"));
            conf.setPassword(prop.getProperty("DB_PWD"));
            conf.setInitialSize(Integer.parseInt(prop.getProperty("DS_INITIAL_SIZE")));
            conf.setMaxActive(Integer.parseInt(prop.getProperty("DS_MAX_ACTIVE")));
            conf.setMaxIdle(Integer.parseInt(prop.getProperty("DS_MAX_IDLE")));
            conf.setMinIdle(Integer.parseInt(prop.getProperty("DS_MIN_IDLE")));
            conf.setType(prop.getProperty("DB_TYPE"));
            conf.setKey(prop.getProperty("DB_KEY"));
            confs.add(conf);
        } catch (Exception e) {
            throw new RuntimeException("parse config resource " + name + " error!");
        }
        return confs;
    }

}
