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

import com.creditease.dbus.domain.model.DataTable;
import org.apache.ibatis.annotations.Param;

import java.util.*;

public interface DataTableMapper {
    int insert(DataTable record);

    int updateByPrimaryKey(DataTable record);

    int confirmVerChange(Integer tableId);

    int updateVerId(@Param("verId") Integer verId, @Param("tableId") int tableId);

    /**
     * schemaName,tableName模糊查询
     *
     * @param dsId
     * @param schemaId
     * @param tableName
     * @return
     */
    List<DataTable> findTables(@Param("dsId") Integer dsId, @Param("schemaId") Integer schemaId,
                               @Param("schemaName") String schemaName, @Param("tableName") String tableName);

    DataTable findById(Integer id);

    List<DataTable> findByStatus(String status);

    List<DataTable> findBySchemaID(Integer schemaID);

    List<DataTable> findByDsIdAndSchemaName(@Param("dsID") Integer dsID, @Param("schemaName") String schemaName);

    int deleteByTableId(Integer id);

    List<Map<String, Object>> getDSList();

    DataTable getByDsSchemaTableName(@Param("dsName") String dsName, @Param("schemaName") String schemaName,
                                     @Param("tableName") String tableName);

    DataTable findBySchemaIdTableName(@Param("schemaId") Integer schemaId, @Param("tableName") String tableName);

    Map<String, Object> getFlowLineCheckInfo(Integer tableId);

    List<DataTable> findAllTables();

    List<HashMap<String, Object>> findTablesByUserId(Integer userId);

    void inactiveTableByDsId(Integer dsId);

    void inactiveTableBySchemaId(Integer schemaId);

    List<DataTable> findActiveTablesByDsId(Integer dsId);

    List<DataTable> findActiveTablesBySchemaId(Integer schemaId);

    int startOrStopTableByTableIds(@Param("list") ArrayList<Integer> ids, @Param("status") String status);

    List<Map<String, Object>> getDataSourcesByTableIds(ArrayList<Integer> ids);

    List<DataTable> searchTableByIds(@Param("list") ArrayList<Integer> ids);

    void updateByTableIds(@Param("dsId") Integer dsId, @Param("schemaId") Integer schemaId, @Param("schemaName") String schemaName,
                          @Param("outputTopic") String outputTopic, @Param("ids") List<Integer> tableIds);

    List<DataTable> findTablesByDsNames(@Param("set") Set<String> dsNames);
}
