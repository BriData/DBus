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


package com.creditease.dbus.stream.db2.appender.bolt.processor.vo;

import com.creditease.dbus.stream.common.appender.bean.MetaVersion;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhenlinzhong
 * @date 2018/10/16 21:17
 */
public class AppenderDataWrapper {
    private List<AppenderDataResults> results = new ArrayList<>();

    private MetaVersion version;


    public List<AppenderDataResults> getResults() {
        return results;
    }

    public void setResults(List<AppenderDataResults> results) {
        this.results = results;
    }

    public MetaVersion getVersion() {
        return version;
    }

    public void setVersion(MetaVersion version) {
        this.version = version;
    }

}
