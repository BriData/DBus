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

import java.util.List;
import java.util.Map;

import com.creditease.dbus.domain.model.ProjectResource;
import org.apache.ibatis.annotations.Param;

public interface ProjectResourceMapper {
    int deleteByPrimaryKey(Integer id);

    int insert(ProjectResource record);

    ProjectResource selectByPrimaryKey(Integer id);

    List<ProjectResource> selectAll();

    int updateByPrimaryKey(ProjectResource record);

    List<Map<String, Object>> selectByProjectId(int projectId);

    int deleteByProjectId(Integer id);

    List<Map<String,Object>> search(Map<String, Object> params);

    List<Map<String,Object>> searchTableResource(Map<String, Object> params);

    String searchStatus(@Param("projectId") int projectId,@Param("tableId") int tableId);

    String searchEncodeType(@Param("tableId") Integer tableId, @Param("projectId")Integer projectId);

    List<Map<String, Object>> selectColumns(Integer tableId);

    List<Map<String,Object>> getDSNames(@Param(value = "projectId") Integer projectId);

    List<Map<String,Object>> getProjectNames();

    ProjectResource selectByPIdTId(@Param("projectId") Integer projectId,@Param("tableId") Integer tableId);

}
