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

package com.creditease.dbus.ws.service;

import com.creditease.dbus.ws.domain.DataTable;
import com.creditease.dbus.ws.mapper.TableMapper;
import com.creditease.dbus.ws.service.mybatis.MybatisTemplate;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;

import java.util.List;
import java.util.Map;

public class TablesService {
    private MybatisTemplate template = MybatisTemplate.template();

    /**
     * service工厂方法
     * @return 返回TablesService实例
     */
    public static TablesService getService() {
        return new TablesService();
    }

    /**
     * 根据ID获取指定table
     * @param id 数据源ID
     * @return 指定的数据源
     */
    public DataTable getTableById(long id) {
        return template.query((session, args) -> {
            TableMapper tableMapper = session.getMapper(TableMapper.class);
            return tableMapper.findById(id);
        });
    }

    /**
     * 根据状态查询相关table
     * @param status 状态,不存在则查询所有table
     * @return 返回满足条件的table列表
     */
    public List<DataTable> findTablesByDsID(String status) {
        if(status!=null) {
            return getTableByStatus(status);
        }

        return findAll();
    }

    /**
     * 根据数据源ID查询相关table
     * @param dsID 数据源ID,不存在则查询所有table
     * @return 返回满足条件的table列表
     */
    public List<DataTable> findTablesByDsID(long dsID) {
        if(dsID!=0) {
            return getTableByDsID(dsID);
        }

        return findAll();
    }

    /**
     * 根据schema的ID查询相关table
     * @param schemaID 数据源ID,不存在则查询所有table
     * @return 返回满足条件的table列表
     */
    public List<DataTable> findTablesBySchemaID(long schemaID) {
        if(schemaID!=0) {
            return getTableBySchemaID(schemaID);
        }

        return findAll();
    }

    /**
     * 根据table的name查询相关table
     * @return 返回满足条件的table列表
     */
    /*
    public List<DataTable> findTablesByTableName() {
        if(tableName!=null) {
            return getTableByTableName(tableName);
        }

        return findAll();
    }
    */

    public List<DataTable> findAllTables() {
        return findAll();
    }

    /**
     * 根据tablename以及schemaName查询相关table
     * @param schemaName,tableName table的名称,不存在则查询所有table
     * @return 返回满足条件的table列表
     */
    public List<DataTable> findTables(Long dsID,String schemaName,String tableName) {

            return getTables(dsID,schemaName,tableName);

    }

    /**
     * 查找verId为空的相关table
     * @return 返回满足条件的table列表
     */
    public List<DataTable> findTablesNoVer(Long schemaId) {

        return getTablesNoVer(schemaId);

    }
    /**
     * 查询数据库中所有table,不分页
     * @return table列表
     */
    public List<DataTable> findAll() {
        return template.query((session, args) -> {
            TableMapper tableMapper = session.getMapper(TableMapper.class);
            return tableMapper.findAll();
        });
    }
    /**
     * 根据状态获取指定的table
     * @param status 状态
     * @return 指定的table
     */
    public List<DataTable> getTableByStatus(String status) {
        return template.query((session, args) -> {
            TableMapper tableMapper = session.getMapper(TableMapper.class);

            // 设置分页
            PageHelper.startPage(1, 5);
            List<DataTable> tables = tableMapper.findByStatus(status);
            // 分页结果
            PageInfo page = new PageInfo(tables);
            return tables;
        });
    }

    /**
     * 根据数据源ID获取指定的table
     * @param dsID 数据源ID
     * @return 指定的table
     */
    public List<DataTable> getTableByDsID(long dsID) {
        return template.query((session, args) -> {
            TableMapper tableMapper = session.getMapper(TableMapper.class);
            return tableMapper.findByDsID(dsID);
        });
    }

    /**
     * 根据schema的ID获取指定的table
     * @param schemaID 数据源ID
     * @return 指定的table
     */
    public List<DataTable> getTableBySchemaID(long schemaID) {
        return template.query((session, args) -> {
            TableMapper tableMapper = session.getMapper(TableMapper.class);
            return tableMapper.findBySchemaID(schemaID);
        });
    }

    /**
     * 根据table的name模糊查询匹配的table
     * @param tableName table的名称
     * @return 指定的table
     */
    public List<DataTable> getTableByTableName(String tableName) {
        return template.query((session, args) -> {
            TableMapper tableMapper = session.getMapper(TableMapper.class);
            return tableMapper.findByTableName(tableName);
        });
    }

    public List<DataTable> findTablesByDsIdAndSchemaName(long dsID, String schemaName) {
        return template.query((session, args) -> {
            TableMapper tableMapper = session.getMapper(TableMapper.class);
            return tableMapper.findByDsIdAndSchemaName(dsID, schemaName);
        });
    }

    public List<DataTable> getTables(Long dsID,String schemaName,String tableName) {
        return template.query((session, args) -> {
            TableMapper tableMapper = session.getMapper(TableMapper.class);
            return tableMapper.findTables(dsID,schemaName,tableName);
        });
    }

    /**
     * 查找verId为空的相关table
     * @return 返回满足条件的table列表
     */
    public List<DataTable> getTablesNoVer(Long schemaId) {
        return template.query((session, args) -> {
            TableMapper tableMapper = session.getMapper(TableMapper.class);
            return tableMapper.findTablesNoVer(schemaId);
        });

    }

    /**
     * 插入新的tables对象
     * @param dt 数据源对象
     * @return 返回插入到数据库的记录数
     */

    public int insertDataTables(DataTable dt) {
        return template.update((session, args) -> {
            TableMapper tableMapper = session.getMapper(TableMapper.class);
            return tableMapper.insert(dt);
        });
    }

    /**
     * 根据id更新table
     * @param
     * @param dt table各个字段的新值
     * @return 更新的记录数
     */

    public int updateDataTables(DataTable dt) {
        return template.query((session, args) -> {
            TableMapper tableMapper = session.getMapper(TableMapper.class);
            return tableMapper.update(dt);
        });
    }

    /**
     * 激活指定的table,使该table生效
     * @param id 表单table的ID
     */
    public void activateDataTable(long id) {
        template.update((session, args) -> {
            TableMapper tableMapper = session.getMapper(TableMapper.class);
            return tableMapper.changeStatus(id, DataTable.OK);
        });
    }

    /**
     * 使table无效
     * @param id 表单table的ID
     */
    public void deactivateDataTable(long id) {
        template.update((session, args) -> {
            TableMapper tableMapper = session.getMapper(TableMapper.class);
            return tableMapper.changeStatus(id, DataTable.ABORT);
        });
    }

    public PageInfo<DataTable> search(int pageNum, int pageSize, Map<String, Object> param) {
        PageHelper.startPage(pageNum, pageSize);
        List<DataTable> list = template.query((session, args) -> {
            TableMapper mapper = session.getMapper(TableMapper.class);
            // 设置分页
            return mapper.search(param);
        });
        // 分页结果
        PageInfo<DataTable> page = new PageInfo<>(list);
        return page;
    }

    public List<DataTable> search(Map<String, Object> param) {
        return template.query((session, args) -> {
            TableMapper mapper = session.getMapper(TableMapper.class);
            // 设置分页
            return mapper.search(param);
        });
    }

    public int deleteTable(int tableId) {
        int result = template.query((session, args) -> {
            TableMapper mapper = session.getMapper(TableMapper.class);
            return mapper.deleteByTableId(tableId);
        });
        return result;
    }
}
