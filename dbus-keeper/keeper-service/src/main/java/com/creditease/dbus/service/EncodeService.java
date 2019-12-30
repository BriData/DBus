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


package com.creditease.dbus.service;

import com.creditease.dbus.domain.mapper.ProjectTableEncodeColumnsMapper;
import com.creditease.dbus.domain.model.ProjectTableEncodeColumns;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class EncodeService {

    @Autowired
    private ProjectTableEncodeColumnsMapper columnMapper;

    private Logger logger = LoggerFactory.getLogger(getClass());

    public PageInfo<ProjectTableEncodeColumns> searchEncodeColumns(Integer pageNum, Integer pageSize, Integer projectId,
                                                                   Integer topoId, Integer dsId, String schemaName, String tableName) {
        PageHelper.startPage(pageNum, pageSize);
        return new PageInfo(columnMapper.search(projectId, topoId, dsId, schemaName, tableName));
    }

}
