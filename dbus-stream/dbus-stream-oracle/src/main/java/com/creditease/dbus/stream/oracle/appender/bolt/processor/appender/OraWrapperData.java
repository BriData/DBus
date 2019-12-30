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


package com.creditease.dbus.stream.oracle.appender.bolt.processor.appender;

import com.creditease.dbus.stream.common.appender.utils.PairWrapper;

/**
 * Created by zhangyf on 18/1/5.
 */
public class OraWrapperData {
    private PairWrapper<String, Object> dataWrapper;
    private PairWrapper<String, Object> beforeDataWrapper;

    public OraWrapperData() {
    }

    public OraWrapperData(PairWrapper<String, Object> dataWrapper, PairWrapper<String, Object> beforeDataWrapper) {
        this.dataWrapper = dataWrapper;
        this.beforeDataWrapper = beforeDataWrapper;
    }

    public PairWrapper<String, Object> getDataWrapper() {
        return dataWrapper;
    }

    public void setDataWrapper(PairWrapper<String, Object> dataWrapper) {
        this.dataWrapper = dataWrapper;
    }

    public PairWrapper<String, Object> getBeforeDataWrapper() {
        return beforeDataWrapper;
    }

    public void setBeforeDataWrapper(PairWrapper<String, Object> beforeDataWrapper) {
        this.beforeDataWrapper = beforeDataWrapper;
    }
}
