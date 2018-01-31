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

import com.creditease.dbus.ws.domain.DataTable;
import com.creditease.dbus.ws.domain.DbusDataSource;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

public interface TableMapper {
    List<DataTable> findAll();
    DataTable findById(long id);
    List<DataTable> findByStatus(String status);
    List<DataTable> findByDsID(long dsID);
    List<DataTable> findBySchemaID(long schemaID);
    List<DataTable> findByTableName(String tableName);
    List<DataTable> findByDsIdAndSchemaName(@Param("dsID") Long dsID, @Param("schemaName")String schemaName);
    //List<DataTable> findBySchemaAndName(@Param("schemaid")long schemaID, @Param("tableName")String tableName);
    //List<DataTable> findByDsAndName(@Param("dsid")long dsID, @Param("tableName")String tableName);
    List<DataTable>  findTables(@Param("dsID")Long dsID,@Param("schemaName")String schemaName,@Param("tableName")String tableName);
    List<DataTable>  findTablesNoVer(long schemaId);
    List<DataTable> search(Map<String, Object> param);
    int insert(DataTable table);
    int update(DataTable table);
    int changeStatus(@Param("id")Long id, @Param("status")String status);
    int confirmVerChange(@Param("id")Long id);
    int deleteByTableId(int id);
}
