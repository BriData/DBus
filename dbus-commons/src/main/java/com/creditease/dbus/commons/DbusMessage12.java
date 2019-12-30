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


package com.creditease.dbus.commons;


/**
 * Created by dongwang47 on 2018/1/5.
 */
public class DbusMessage12 extends DbusMessage {
    public DbusMessage12() {
    }

    public DbusMessage12(String version, ProtocolType type, String schemaNs) {
        super(version, type);
        this.schema = new Schema12(schemaNs);
    }

    public static class Schema12 extends Schema {
        public Schema12() {
        }

        public Schema12(String schemaNs) {
            super(schemaNs);
        }
    }
}
