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

import com.creditease.dbus.ws.domain.DbusDataSource;
import com.creditease.dbus.ws.mapper.DataSourceMapper;
import com.creditease.dbus.ws.service.mybatis.MybatisTemplate;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 执行数据模型处理的service层
 * Created by Shrimp on 16/9/1.
 */
public class DataSourceService {
    private MybatisTemplate template = MybatisTemplate.template();

    /**
     * service工厂方法
     *
     * @return 返回DataSourceService实例
     */
    public static DataSourceService getService() {
        return new DataSourceService();
    }

    /**
     * 根据ID获取指定数据源
     *
     * @param id 数据源ID
     * @return 指定的数据源
     */
    public DbusDataSource getDataSourceById(long id) {
        return template.query((session, args) -> {
            DataSourceMapper dataSourceMapper = session.getMapper(DataSourceMapper.class);
            return dataSourceMapper.findById(id);
        });
    }

    /**
     * 根据数据源名称查询数据源
     *
     * @param name 数据源名称,不存在则查询所有数据源
     * @return 返回满足条件的数据源列表
     */
    public List<DbusDataSource> findDataSources(String name) {
        if (name != null) {
            List<DbusDataSource> list = new ArrayList<>();
            list.add(getDataSourceByName(name));
            return list;
        }

        return findAll();
    }

    /**
     * 查询数据库中所有数据源,不分页
     *
     * @return 数据源列表
     */
    public List<DbusDataSource> findAll() {
        return template.query((session, args) -> {
            DataSourceMapper dataSourceMapper = session.getMapper(DataSourceMapper.class);
            return dataSourceMapper.findAll();
        });
    }

    /**
     * 根据数据源名字获取指定的唯一一个数据源
     *
     * @param name 数据源名称
     * @return 指定的数据源
     */
    public DbusDataSource getDataSourceByName(String name) {
        return template.query((session, args) -> {
            DataSourceMapper dataSourceMapper = session.getMapper(DataSourceMapper.class);
            return dataSourceMapper.findByDsName(name);
        });
    }

    /**
     * 生成新的数据源对象
     *
     * @param ds 数据源对象
     * @return 返回插入到数据库的记录数
     */
    public int insertDataSource(DbusDataSource ds) {
        return template.update((session, args) -> {
            DataSourceMapper dataSourceMapper = session.getMapper(DataSourceMapper.class);
            return dataSourceMapper.insert(ds);
        });
    }


    public int updateDataSource(DbusDataSource ds) {
        return template.query((session, args) -> {
            DataSourceMapper dataSourceMapper = session.getMapper(DataSourceMapper.class);
            return dataSourceMapper.update(ds);
        });
    }

    /**
     * 激活指定的数据源,使该数据源生效
     *
     * @param id 数据源ID
     */
    public void activateDataSource(long id) {
        template.update((session, args) -> {
            DataSourceMapper dataSourceMapper = session.getMapper(DataSourceMapper.class);
            return dataSourceMapper.changeStatus(id, DbusDataSource.ACTIVE);
        });
    }

    /**
     * 使数据源无效
     *
     * @param id 数据源ID
     */
    public void deactivateDataSource(long id) {
        template.update((session, args) -> {
            DataSourceMapper dataSourceMapper = session.getMapper(DataSourceMapper.class);
            return dataSourceMapper.changeStatus(id, DbusDataSource.INACTIVE);
        });
    }

    /**
     * 查询ds列表,分页
    */

    public PageInfo<DbusDataSource> search(int pageNum, int pageSize,Map<String, Object> param) {
        PageHelper.startPage(pageNum, pageSize);
        List<DbusDataSource> list = template.query((session, args) -> {
            DataSourceMapper mapper = session.getMapper(DataSourceMapper.class);
            // 设置分页
            return mapper.search(param);
        });
        // 分页结果
        
        PageInfo<DbusDataSource> page = new PageInfo<>(list);
        return page;
    }

}
