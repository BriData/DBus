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
public class DbusMessage13 extends DbusMessage {
    public DbusMessage13() {
    }

    public DbusMessage13(String version, ProtocolType type, String schemaNs, int batchNo) {
        super(version, type);
        this.schema = new Schema13(schemaNs, batchNo);
    }

    public static class Schema13 extends Schema {
        private int batchId;

        public Schema13() {
        }

        public Schema13(String schemaNs, int batchNo) {
            super(schemaNs);
            this.batchId = batchNo;
        }

        public int getBatchId() {
            return batchId;
        }
    }
}
