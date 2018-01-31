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

import com.creditease.dbus.ws.domain.AvroSchema;
import com.creditease.dbus.ws.domain.DataSchema;
import com.creditease.dbus.ws.mapper.AvroSchemaMapper;
import com.creditease.dbus.ws.mapper.DataSchemaMapper;
import com.creditease.dbus.ws.service.mybatis.MybatisTemplate;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;

import java.util.List;
import java.util.Map;

/**
 * 执行数据模型处理的service层
 */
public class DataSchemaService {
    private MybatisTemplate template = MybatisTemplate.template();

    /**
     * service工厂方法
     * @return 返回DataSchemaService实例
     */
    public static DataSchemaService getService() {
        return new DataSchemaService();
    }

    public PageInfo<DataSchema> search(int pageNum, int pageSize, Map<String, Object> param) {
        PageHelper.startPage(pageNum, pageSize);
        List<DataSchema> list = template.query((session, args) -> {
            DataSchemaMapper mapper = session.getMapper(DataSchemaMapper.class);
            // 设置分页
            return mapper.search(param);
        });
        // 分页结果
        PageInfo<DataSchema> page = new PageInfo<>(list);
        return page;
    }

    /**
     * 根据ID获取指定DataSchema
     * @param id DataSchema的ID
     * @return 指定的DataSchema
     */
    public DataSchema getDataSchemaById(long id) {
        return template.query((session, args) -> {
            DataSchemaMapper DataSchemaMapper = session.getMapper(DataSchemaMapper.class);
            return DataSchemaMapper.findById(id);
        });
    }

    /**
     * 根据DataSchema名称查询DataSchema
     * @param name DataSchema名称,不存在则查询所有DataSchema
     * @return 返回满足条件的DataSchema列表
     */
    public List<DataSchema> findDataSchemas(String name) {
        if(name != null) {
            return getDataSchemaByName(name);
        }
        return findAll();
    }


    public List<DataSchema> checkDataSchema(Long dsId,String schemaName) {
            return getDataSchemaBydsIdAndschemaName(dsId,schemaName);
    }

    /**
     * 查询数据库中所有DataSchema,不分页
     * @return DataSchema列表
     */
    public List<DataSchema> findAll() {
        return template.query((session, args) -> {
            DataSchemaMapper dataSchemaMapper = session.getMapper(DataSchemaMapper.class);
            return dataSchemaMapper.findAll();
        });
    }

    public List<DataSchema> findAllAll() {
        return template.query((session, args) -> {
            DataSchemaMapper dataSchemaMapper = session.getMapper(DataSchemaMapper.class);
            return dataSchemaMapper.findAllAll();
        });
    }


    public List<DataSchema> findByDsId(long dsId) {
        return template.query((session, args) -> {
            DataSchemaMapper dataSchemaMapper = session.getMapper(DataSchemaMapper.class);
            return dataSchemaMapper.findByDsId(dsId);
        });
    }



    /**
     * 根据数据源名字获取指定的唯一一个数据源
     * @param name 数据源名称
     * @return 指定的数据源
     */
    public List<DataSchema> getDataSchemaByName(String name) {
        return template.query((session, args) -> {
            DataSchemaMapper dataSchemaMapper = session.getMapper(DataSchemaMapper.class);
            return dataSchemaMapper.findBySchemaName(name);
        });
    }


    public List<DataSchema> getDataSchemaBydsIdAndschemaName(Long dsId, String schemaName) {
        return template.query((session, args) -> {
            DataSchemaMapper dataSchemaMapper = session.getMapper(DataSchemaMapper.class);
            return dataSchemaMapper.getDataSchemaBydsIdAndschemaName(dsId,schemaName);
        });
    }

    /**
     * 生成新的数据源对象
     * @param ds 数据源对象
     * @return 返回插入到数据库的记录数
     */
    public int insertDataSchema(DataSchema ds) {
        return template.update((session, args) -> {
            DataSchemaMapper dataSchemaMapper = session.getMapper(DataSchemaMapper.class);
            return dataSchemaMapper.insert(ds);
        });
    }

    /**
     * 根据id更新DataSchema
     * @param
     * @param ds DataSchema各个字段的新值
     * @return 更新的记录数
     */
    public int updateDataSchema(DataSchema ds) {
        return template.query((session, args) -> {
            DataSchemaMapper dataSchemaMapper = session.getMapper(DataSchemaMapper.class);
            return dataSchemaMapper.update(ds);
        });
    }

    /**
     * 激活指定的DataSchema,使DataSchema生效
     * @param id DataSchema ID
     */
    public void activateDataSchema(long id) {
        template.update((session, args) -> {
            DataSchemaMapper dataSchemaMapper = session.getMapper(DataSchemaMapper.class);
            return dataSchemaMapper.changeStatus(id, DataSchema.ACTIVE);
        });
    }

    /**
     * 使DataSchema无效
     * @param id DataSchema ID
     */
    public void deactivateDataSchema(long id) {
        template.update((session, args) -> {
            DataSchemaMapper dataSchemaMapper = session.getMapper(DataSchemaMapper.class);
            return dataSchemaMapper.changeStatus(id, DataSchema.INACTIVE);
        });
    }


    public int deleteSchema(int schemaId) {
        int result = template.query((session, args) -> {
            DataSchemaMapper mapper = session.getMapper(DataSchemaMapper.class);
            return mapper.deleteBySchemaId(schemaId);
        });
        return result;
    }
}
