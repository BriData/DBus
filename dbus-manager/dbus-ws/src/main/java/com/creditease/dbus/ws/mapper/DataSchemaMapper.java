/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2017 Bridata
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

package com.creditease.dbus.ws.mapper;

import com.creditease.dbus.ws.domain.AvroSchema;
import com.creditease.dbus.ws.domain.DataSchema;
import com.creditease.dbus.ws.domain.DbusDataSource;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

public interface DataSchemaMapper {
    List<DataSchema> findAll();
    List<DataSchema> findAllAll();
    DataSchema findById(Long id);
    List<DataSchema> findByDsId(Long dsId);
    List<DataSchema> findBySchemaName(String schemaName);
    List<DataSchema> getDataSchemaBydsIdAndschemaName(@Param("dsId") Long dsId, @Param("schemaName")String schemaName);
    int insert(DataSchema dataSchema);
    int update(DataSchema dataSchema);
    int changeStatus(@Param("id")long id, @Param("status")String status);
    List<DataSchema> search(Map<String, Object> param);
    int deleteBySchemaId(int id);
}
