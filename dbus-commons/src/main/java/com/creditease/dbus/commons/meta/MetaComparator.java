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


package com.creditease.dbus.commons.meta;

import com.creditease.dbus.commons.MetaWrapper;

/**
 * Created by zhangyf on 17/3/21.
 */
public interface MetaComparator {
    public MetaCompareResult compare(MetaWrapper m1, MetaWrapper m2);

    public static enum FiledCompareResult {
        ADD_FIELD(1, "adding field"), DROP_FIELD(2, "drop field"), TYPE_CHANGED(3, "field type changed");
        private String description;
        private int result;

        private FiledCompareResult(int result, String description) {
            this.result = result;
            this.description = description;
        }

        public String getDescription() {
            return description;
        }

        public int getResult() {
            return result;
        }

        @Override
        public String toString() {
            return description;
        }
    }
}
