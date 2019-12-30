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


package com.creditease.dbus.controller;

import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.domain.model.ProjectSink;
import com.creditease.dbus.service.ProjectSinkService;
import com.creditease.dbus.service.ProjectTableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * Created by mal on 2018/3/21.
 */
@RestController
@RequestMapping("/projectSink")
public class ProjectSinkController extends BaseController {

    @Autowired
    private ProjectSinkService service;

    @Autowired
    private ProjectTableService tableService;

    @PostMapping("/insertAll")
    public ResultEntity insertAll(@RequestBody List<ProjectSink> projectSinks) {
        service.insertAll(projectSinks);
        return resultEntityBuilder().build();
    }

    @PostMapping("/insert")
    public ResultEntity insert(@RequestBody ProjectSink projectSink) {
        int cnt = service.insert(projectSink);
        return resultEntityBuilder().payload(cnt).build();
    }

    @GetMapping("/select-by-project-id/{id}")
    public ResultEntity selectByProjectId(@PathVariable int id) {
        return resultEntityBuilder().payload(service.selectByProjectId(id)).build();
    }

    @PostMapping("/updateAll")
    public ResultEntity updateAll(@RequestBody List<ProjectSink> projectSinks) {
        service.updateAll(projectSinks);
        return resultEntityBuilder().build();
    }

    @GetMapping("/delete-by-project-id/{id}")
    public ResultEntity deleteByProjectId(@PathVariable int id) {
        return resultEntityBuilder().payload(service.deleteByProjectId(id)).build();
    }

    @GetMapping("/getExistedTopicsByProjectId/{projectId}")
    public ResultEntity getExistedTopicsByProjectId(@PathVariable Long projectId) {
        return resultEntityBuilder().payload(tableService.getExistedTopicsByProjectId(projectId)).build();
    }
}
