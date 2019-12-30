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


package com.creditease.dbus.extractor.vo;

public class OutputTopicVo {
    private String dsName;
    private String dsType;
    private String topic;
    private String controlTopic;

    public String getDsName() {
        return dsName;
    }

    public void setDsName(String dsName) {
        this.dsName = dsName;
    }

    public String getDsType() {
        return dsType;
    }

    public void setDsType(String dsType) {
        this.dsType = dsType;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getControlTopic() {
        return controlTopic;
    }

    public void setControlTopic(String controlTopic) {
        this.controlTopic = controlTopic;
    }

    @Override
    public String toString() {
        return "OutputTopicVo [dsName=" + dsName + ", dsType=" + dsType + ", topic=" + topic + ", ctrl_topic=" + controlTopic + "]";
    }


}
