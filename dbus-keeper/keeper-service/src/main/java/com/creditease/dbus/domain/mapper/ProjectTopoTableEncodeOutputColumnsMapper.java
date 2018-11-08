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

import com.creditease.dbus.domain.model.ProjectTopoTableEncodeOutputColumns;

import org.apache.ibatis.annotations.Param;

public interface ProjectTopoTableEncodeOutputColumnsMapper {
	int deleteByPrimaryKey(Integer id);

	int insert(ProjectTopoTableEncodeOutputColumns record);

	ProjectTopoTableEncodeOutputColumns selectByPrimaryKey(Integer id);

	List<ProjectTopoTableEncodeOutputColumns> selectAll();

	int updateByPrimaryKey(ProjectTopoTableEncodeOutputColumns record);

	List<Map<String, Object>> selectByTableId(Integer tableId);

	int deleteByTopoTableId(@Param("tableId") Integer tableId);

	ProjectTopoTableEncodeOutputColumns selectByTopoTableIdAndFieldName(@Param("topoTableId") Integer topoTableId,
																		@Param("fieldName") String fieldName);

	int deleteByProjectId(Integer projectId);

	List<Map<String, Object>> search(@Param("projectId") Integer projectId, @Param("topoId") Integer topoId, @Param("dsId") Integer dsId,
									 @Param("schemaName") String schemaName, @Param("tableName") String tableName);

    List<ProjectTopoTableEncodeOutputColumns> selectByTopoTableId(@Param("topoTableId") Integer topoTableId);

}
