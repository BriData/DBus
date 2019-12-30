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

import com.creditease.dbus.commons.meta.MetaComparator.FiledCompareResult;

import java.util.ArrayList;
import java.util.List;

/**
 * meta信息比较结果
 * Created by zhangyf on 17/3/21.
 */
public class MetaCompareResult {
    private List<ResultItem> itemList;

    public MetaCompareResult() {
        itemList = new ArrayList<>();
    }

    public void addResultField(String field, FiledCompareResult res) {
        itemList.add(new ResultItem(field, res.getResult(), res.getDescription()));
    }

    public boolean isCompatible() {
        return itemList.size() == 0;
    }

    public List<ResultItem> getItemList() {
        return itemList;
    }

    public static class ResultItem {
        private String fieldName;
        private int compareResult;
        private String description;

        public ResultItem(String fieldName, int compareResult, String description) {
            this.fieldName = fieldName;
            this.compareResult = compareResult;
            this.description = description;
        }

        public String getFieldName() {
            return fieldName;
        }

        public int getCompareResult() {
            return compareResult;
        }

        public String getDescription() {
            return description;
        }
    }
}
