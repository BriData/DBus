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


package com.creditease.dbus.stream.mysql.appender.meta;

import com.creditease.dbus.stream.common.appender.bean.MetaVersion;

public class MysqlMetaVersion extends MetaVersion {
    private boolean needCompare; //是否需要比较，发生ddl事件需要进行meta信息比较，判断meta信息是否兼容，从而决定是否升版本

    public boolean isNeedCompare() {
        return needCompare;
    }

    public void setNeedCompare(boolean needCompare) {
        this.needCompare = needCompare;
    }
}
