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

import com.creditease.dbus.domain.mapper.ProjectEncodeHintMapper;
import com.creditease.dbus.domain.model.ProjectEncodeHint;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by mal on 2018/3/27.
 */
@Service
public class ProjectEncodeHintService {

    @Autowired
    private ProjectEncodeHintMapper mapper;

    public int insert(ProjectEncodeHint projectEncodeHint) {
        return mapper.insert(projectEncodeHint);
    }

    public void insertAll(List<ProjectEncodeHint> projectEncodeHints) {
        for (ProjectEncodeHint projectEncodeHint : projectEncodeHints) {
            insert(projectEncodeHint);
        }
    }

    public Map<Integer, List<Map<String, Object>>> selectByProjectId(int id) {
        Map<Integer, List<Map<String, Object>>> retMap = null;
        List<Map<String, Object>> list = mapper.selectByProjectId(id);
        if (!CollectionUtils.isEmpty(list)) {
            retMap = new HashMap<>();
            for (Map<String, Object> item : list) {
                if (retMap.containsKey(item.get("tid"))) {
                    retMap.get(item.get("tid")).add(item);
                } else {
                    List<Map<String, Object>> wkList = new ArrayList<>();
                    wkList.add(item);
                    retMap.put((Integer) item.get("tid"), wkList);
                }
            }
        }
        return retMap;
    }

    public int update(ProjectEncodeHint projectEncodeHint) {
        return mapper.updateByPrimaryKey(projectEncodeHint);
    }

    public void updateAll(List<ProjectEncodeHint> projectEncodeHints) {
        for (ProjectEncodeHint projectEncodeHint : projectEncodeHints) {
            update(projectEncodeHint);
        }
    }

    public int deleteByProjectId(int id) {
        return mapper.deleteByProjectId(id);
    }

    public List<Map<String, Object>> selectByPidAndTid(int projectId, int tableId) {
        return mapper.selectByPidAndTid(projectId, tableId);
    }

}
