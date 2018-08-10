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

package com.creditease.dbus.controller;

import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.base.ResultEntityBuilder;
import com.creditease.dbus.domain.model.FullPullHistory;
import com.creditease.dbus.service.FullPullHistoryService;
import com.github.pagehelper.PageInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Created by xiancangao on 2018/04/16.
 */
@RestController
@RequestMapping("/fullPullHistory")
public class FullPullHistoryController extends BaseController {

    @Autowired
    private FullPullHistoryService service;

    @GetMapping("search")
    public ResultEntity search(Integer pageNum, Integer pageSize, FullPullHistory history) {
        ResultEntityBuilder reb = resultEntityBuilder();
        PageInfo<FullPullHistory> page = service.search(pageNum, pageSize, currentUserId(), currentUserRole(), history);
        return reb.payload(page).build();
    }

    @GetMapping("/datasourceNames")
    public ResultEntity getDatasourceNames(@RequestParam(required = false) Integer projectId) {
        List<Map<String, Object>> dsNames = service.getDatasourceNames(currentUserId(), currentUserRole(), projectId);
        return resultEntityBuilder().payload(dsNames).build();
    }

    @GetMapping("/project-names")
    public ResultEntity getProjectNames(@RequestParam(required = false) Integer projectId) {
        return resultEntityBuilder().payload(service.getProjectNames(currentUserId(), currentUserRole(), projectId)).build();
    }

    @PostMapping(path = "create", consumes = "application/json")
    public ResultEntity insert(@RequestBody FullPullHistory history) {
        long id = service.insert(history);
        return resultEntityBuilder().payload(id).build();
    }

    @PostMapping(path = "update", consumes = "application/json")
    public ResultEntity update(@RequestBody FullPullHistory history) {
        service.update(history);
        return resultEntityBuilder().build();
    }
}
