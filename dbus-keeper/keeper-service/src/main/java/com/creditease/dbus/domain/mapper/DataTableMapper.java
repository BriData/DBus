/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2018 Bridata
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.creditease.dbus.domain.model.DataTable;

import org.apache.ibatis.annotations.Param;

public interface DataTableMapper {
    int insert(DataTable record);

    int updateByPrimaryKey(DataTable record);

    int confirmVerChange(int tableId);

    int updateVerId(@Param("verId") int verId, @Param("tableId") int tableId);

    List<DataTable> findTables(@Param("dsId") Integer dsID, @Param("schemaName") String schemaName, @Param("tableName") String tableName);

    DataTable findById(int id);

    List<DataTable> findByStatus(String status);

    List<DataTable> findByDsID(int dsID);

    List<DataTable> findBySchemaID(int schemaID);

    List<DataTable> findByDsIdAndSchemaName(@Param("dsID") Integer dsID, @Param("schemaName") String schemaName);

    List<DataTable> findTablesNoVer(int schemaId);

    int changeStatus(@Param("id") Long id, @Param("status") String status);

    int deleteByTableId(int id);

    List<Map<String, Object>> getDSList();

    DataTable getByDsSchemaTableName(@Param("dsName") String dsName, @Param("schemaName") String schemaName,
                                     @Param("tableName") String tableName);
    DataTable findBySchemaIdTableName(@Param("schemaId") int schemaId,@Param("tableName") String tableName);

    Map<String, Object> getFlowLineCheckInfo(Integer tableId);

    List<DataTable> findAllTables();

    List<HashMap<String,Object>> findTablesByUserId(Integer userId);

    void inactiveTableByDsId(Integer dsId);

    void inactiveTableBySchemaId(Integer schemaId);

    List<DataTable> findActiveTablesByDsId(int dsId);

    List<DataTable> findActiveTablesBySchemaId(int schemaId);

    int startOrStopTableByTableIds(@Param("list") ArrayList<Integer> ids, @Param("status") String status);

    List<Map<String, Object>> isExistByTableName(@Param("tables") String tables,
                                                 @Param("dsIds") String dsIds);

    List<Map<String, Object>> getDataSourcesByTableIds(ArrayList<Integer> ids);

    List<DataTable> searchTableByIds(@Param("list") ArrayList<Integer> ids);
}
