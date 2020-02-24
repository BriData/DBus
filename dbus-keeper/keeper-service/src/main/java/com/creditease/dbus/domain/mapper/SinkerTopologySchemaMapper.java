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

import com.creditease.dbus.domain.model.SinkerTopologySchema;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

public interface SinkerTopologySchemaMapper {
    int deleteByPrimaryKey(Integer id);

    int insert(SinkerTopologySchema record);

    int insertOrUpdate(SinkerTopologySchema record);

    SinkerTopologySchema selectByPrimaryKey(Integer id);

    List<SinkerTopologySchema> selectAll();

    int updateByPrimaryKey(SinkerTopologySchema record);

    List<SinkerTopologySchema> search(Map<String, Object> param);

    List<SinkerTopologySchema> searchAll(@Param("dsName") String dsName, @Param("schemaName") String schemaName);

    int insertMany(@Param("list") List<SinkerTopologySchema> sinkerSchemaList);
}
