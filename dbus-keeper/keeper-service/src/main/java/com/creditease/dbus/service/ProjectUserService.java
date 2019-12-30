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

import com.creditease.dbus.domain.mapper.ProjectUserMapper;
import com.creditease.dbus.domain.model.ProjectUser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Created by mal on 2018/3/27.
 */
@Service
public class ProjectUserService {

    @Autowired
    private ProjectUserMapper mapper;

    public int insert(ProjectUser projectUser) {
        return mapper.insert(projectUser);
    }

    public void insertAll(List<ProjectUser> projectUsers) {
        for (ProjectUser projectUser : projectUsers) {
            insert(projectUser);
        }
    }

    public List<Map<String, Object>> selectByProjectId(int id) {
        return mapper.selectByProjectId(id);
    }

    public int update(ProjectUser projectUser) {
        return mapper.updateByPrimaryKey(projectUser);
    }

    public void updateAll(List<ProjectUser> projectUsers) {
        for (ProjectUser projectUser : projectUsers) {
            update(projectUser);
        }
    }

    public int deleteByProjectId(int id) {
        return mapper.deleteByProjectId(id);
    }

}
