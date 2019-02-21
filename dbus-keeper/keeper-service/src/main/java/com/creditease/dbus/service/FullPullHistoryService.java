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

package com.creditease.dbus.service;

import com.creditease.dbus.domain.mapper.FullPullHistoryMapper;
import com.creditease.dbus.domain.model.FullPullHistory;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by xiancangao on 2018/04/16.
 */
@Service
public class FullPullHistoryService {

    @Autowired
    private FullPullHistoryMapper mapper;

    public PageInfo<FullPullHistory> search(Integer pageNum, Integer pageSize, Integer userId, String roleType, HashMap<String, Object> param) {
        PageHelper.startPage(pageNum, pageSize);
        if(!"admin".equals(roleType)){
            param.put("userId", userId);
        }
        List<FullPullHistory> list = mapper.search(param);
        PageInfo pageInfo = new PageInfo(list);
        return pageInfo;
    }

    public List<Map<String, Object>> getDatasourceNames(Integer userId, String roleType, Integer projectId) {
        HashMap<String, Object> param = new HashMap<>();
        if (!"admin".equals(roleType)) {
            param.put("userId", userId);
            param.put("projectId", projectId);
        }
        return mapper.getDSNames(param);
    }

    public List<Map<String, Object>> getProjectNames(Integer userId, String roleType, Integer projectId) {
        HashMap<String, Object> param = new HashMap<>();
        if (!"admin".equals(roleType)) {
            param.put("userId", userId);
            param.put("projectId", projectId);
        }
        return mapper.getProjectNames(param);
    }

    public long insert(FullPullHistory history) {
        return mapper.insert(history);
    }

    public void update(FullPullHistory history) {
        mapper.updateByPrimaryKey(history);
    }
}
