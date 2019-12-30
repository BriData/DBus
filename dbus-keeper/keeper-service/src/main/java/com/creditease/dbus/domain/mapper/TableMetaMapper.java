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


package com.creditease.dbus.domain.mapper;

import com.creditease.dbus.domain.model.TableMeta;

import java.util.List;
import java.util.Map;

public interface TableMetaMapper {
    int deleteByPrimaryKey(Integer id);

    int insert(TableMeta record);

    TableMeta selectByPrimaryKey(Integer id);

    List<TableMeta> selectAll();

    int updateByPrimaryKey(TableMeta record);

    List<Map<String, Object>> getVersionColumnDetail(Integer verId);

    List<TableMeta> selectByTableId(Integer tableId);
}
