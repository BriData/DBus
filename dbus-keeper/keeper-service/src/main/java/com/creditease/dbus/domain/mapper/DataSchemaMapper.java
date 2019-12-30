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

import com.creditease.dbus.domain.model.DataSchema;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

public interface DataSchemaMapper {
    List<Map<String, Object>> searchSchemaAndDs(Map<String, Object> param);

    List<DataSchema> searchSchema(@Param("schemaId") Integer schemaId, @Param("dsId") Integer dsId, @Param("schemaName") String schemaName);

    int insert(DataSchema record);

    int update(DataSchema record);

    int deleteBySchemaId(Integer id);

    int updateSchemaStatusByPrimaryKey(Map<String, Object> param);

    DataSchema selectById(Integer id);

    DataSchema findByDsIdAndSchemaName(@Param("dsId") Integer dsId, @Param("schemaName") String schemaName);

    void inactiveSchemaByDsId(Integer dsId);
}
