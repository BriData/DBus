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
    private final static String ENCODE_CHAR = "*****";

    public PageInfo<FullPullHistory> search(Integer pageNum, Integer pageSize, Integer userId, String roleType, FullPullHistory history) {
        PageHelper.startPage(pageNum, pageSize);

        HashMap<String, Object> param = new HashMap<>();
        param.put("projectName", history.getProjectName());
        param.put("dsName", history.getDsName());
        param.put("schemaName", history.getSchemaName());
        param.put("tableName", history.getTableName());
        if(!"admin".equals(roleType)){
            param.put("userId", userId);
        }
        List<FullPullHistory> list = mapper.search(param);

        //非admin用户数据脱敏
       /* if (!"admin".equals(roleType)) {
            List<Long> ids = mapper.searchIdsByUid(userId);
            encodeColumus(list, ids);
        }*/
        PageInfo pageInfo = new PageInfo(list);
        return pageInfo;
    }

    private void encodeColumus(List<FullPullHistory> list, List<Long> ids) {
        for (FullPullHistory history : list) {
            if (!ids.contains(history.getId())) {
                history.setTableName(ENCODE_CHAR);
                history.setSchemaName(ENCODE_CHAR);
            }
        }
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
        mapper.updateReqMsgOffset(history);
    }
}
