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

import com.creditease.dbus.domain.model.DataSource;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

public interface DataSourceMapper {
    int deleteByPrimaryKey(Integer id);

    int insert(DataSource record);

    DataSource selectByPrimaryKey(Integer id);

    List<DataSource> selectAll();

    int updateByPrimaryKey(DataSource record);

    List<Map<String,Object>> search(Map<String, Object> param);

    List<DataSource> getDataSourceByName(Map<String, Object> param);

    DataSource getByName(String dsName);

    void updateDsStatusByPrimaryKey(Map<String, Object> param);

    List<Map<String,Object>> getDSNames();

    List<DataSource> getDataSourceByDsTypes(@Param("list") List<String> dsTypes);

    List<DataSource> getDataSourceByDsType(String dsType);

}
