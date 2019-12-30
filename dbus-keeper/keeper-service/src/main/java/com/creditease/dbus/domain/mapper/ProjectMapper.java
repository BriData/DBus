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

import com.creditease.dbus.domain.model.Project;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

public interface ProjectMapper {

    int deleteByPrimaryKey(Integer id);

    int insert(Project record);

    Project selectByPrimaryKey(Integer id);

    List<Project> selectAll();

    int updateByPrimaryKey(Project record);

    List<Map<String, Object>> selectResources(Map<String, Object> param);

    List<Map<String, Object>> selectColumns(Integer id);

    List<Map<String, Object>> selectProjects(Map<String, Object> param);

    List<Map<String, Object>> selectUserRelationProjects(Map<String, Object> param);

    List<Project> getProjectBySinkId(Integer id);

    String getPrincipal(int id);

    List<Project> getMountedProjrct(@Param("dsId") Integer dsId, @Param("tableId") Integer tableId, @Param("sinkId") Integer sinkId);

    List<Project> search(Map<String, Object> map);
}
