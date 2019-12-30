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

import com.creditease.dbus.heartbeat.vo.CommonConfigVo;

public class CommonConfigResource extends FileConfigResource<CommonConfigVo> {

    public CommonConfigResource(String name) {
        super(name);
    }

    @Override
    public CommonConfigVo parse() {
        CommonConfigVo conf = new CommonConfigVo();
        try {
            conf.setSinkerCheckAlarmInterval(prop.getProperty("sinker.check.alarm.interval"));
            conf.setSinkerHeartbeatTopic(prop.getProperty("sinker.heartbeat.topic"));
            conf.setSinkerKafkaOffset(prop.getProperty("sinker.kafka.offset"));
            conf.setFlowLineCheckUrl(prop.getProperty("flow.line.check.url"));
        } catch (Exception e) {
            throw new RuntimeException("parse config resource " + name + " error!");
        }
        return conf;
    }
}
