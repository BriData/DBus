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
import com.creditease.dbus.commons.SupportedDb2DataType;

/**
 * 比较两个版本的meta信息的兼容性
 * Created by zhenlinzhong on 18/6/26.
 */
public class Db2MetaComparator extends AbstractMetaComparator {

    protected boolean isCellSupported(MetaWrapper.MetaCell cell) {
        return SupportedDb2DataType.isSupported(cell.getDataType()) && !cell.isHidden() && !cell.isVirtual();
    }


    protected DataType convert(MetaWrapper.MetaCell cell) {
        return DataType.convertDb2DataType(cell.getDataType());
    }
}
