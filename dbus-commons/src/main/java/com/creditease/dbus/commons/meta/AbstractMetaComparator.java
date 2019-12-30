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

import com.creditease.dbus.commons.DataType;
import com.creditease.dbus.commons.MetaWrapper;
import com.creditease.dbus.commons.MetaWrapper.MetaCell;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 比较两个版本的meta信息的兼容性
 * Created by zhangyf on 17/1/10.
 */
public abstract class AbstractMetaComparator implements MetaComparator {
    @Override
    public MetaCompareResult compare(MetaWrapper originalMeta, MetaWrapper receivedMeta) {
        List<MetaCell> m1cells = filter(originalMeta.getColumns());
        List<MetaCell> m2cells = filter(receivedMeta.getColumns());

        MetaCompareResult result;
        if (m1cells.size() > m2cells.size()) {
            result = compare(receivedMeta, m1cells, FiledCompareResult.DROP_FIELD);
        } else {
            result = compare(originalMeta, m2cells, FiledCompareResult.ADD_FIELD);
        }

        return result;
    }

    private MetaCompareResult compare(MetaWrapper m, List<MetaCell> cells, FiledCompareResult notExistsResult) {
        MetaCompareResult result = new MetaCompareResult();
        for (MetaCell cell : cells) {
            if (m.contains(cell.getColumnName())) {
                MetaCell m1cell = m.get(cell.getColumnName());
                if (isCellSupported(m1cell)) {
                    // 判断转换后类型是否一致
                    if (convert(cell) != convert(m1cell)) {
                        result.addResultField(cell.getColumnName(), FiledCompareResult.TYPE_CHANGED);
                    }
                }
            } else {
                result.addResultField(cell.getColumnName(), notExistsResult);
            }
        }
        return result;
    }

    private List<MetaCell> filter(List<MetaCell> cells) {
        return cells.stream().filter(cell -> isCellSupported(cell)).collect(Collectors.toList());
    }

    protected abstract boolean isCellSupported(MetaCell cell);

    protected abstract DataType convert(MetaCell cell);
}
