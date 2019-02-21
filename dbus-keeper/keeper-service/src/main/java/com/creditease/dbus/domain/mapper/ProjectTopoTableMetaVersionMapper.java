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

import com.creditease.dbus.domain.model.ProjectTopoTableMetaVersion;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface ProjectTopoTableMetaVersionMapper {
    int deleteByPrimaryKey(Integer id);

    int insert(ProjectTopoTableMetaVersion record);

    ProjectTopoTableMetaVersion selectByPrimaryKey(Integer id);

    List<ProjectTopoTableMetaVersion> selectAll();

    int updateByPrimaryKey(ProjectTopoTableMetaVersion record);

    List<ProjectTopoTableMetaVersion> selectByNonPrimaryKey(@Param("projectId") int projectId,
                                                               @Param("topoId") int topoId,
                                                               @Param("tableId") int tableId,
                                                            @Param("version") int version);
    //如果columnName是null,则删除某个projectTopoTable下所有的meta_version
    int deleteByNonPrimaryKey(@Param("projectId") int projectId,
                              @Param("topoId") int topoId,
                              @Param("tableId") int tableId,
                              @Param("columnName") String columnName);

	int deleteByProjectId(int projectId);
}
