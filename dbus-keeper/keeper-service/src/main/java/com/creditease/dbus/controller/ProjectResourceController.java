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
import com.creditease.dbus.domain.model.ProjectResource;
import com.creditease.dbus.service.ProjectResourceService;
import com.github.pagehelper.PageInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Created by mal on 2018/3/21.
 */
@RestController
@RequestMapping("/projectResource")
public class ProjectResourceController extends BaseController {

    @Autowired
    private ProjectResourceService service;

    @PostMapping("/insertAll")
    public ResultEntity insertAll(@RequestBody List<ProjectResource> projectResources) {
        service.insertAll(projectResources);
        return resultEntityBuilder().build();
    }

    @PostMapping("/insert")
    public ResultEntity insert(@RequestBody ProjectResource projectResource) {
        int cnt = service.insert(projectResource);
        return resultEntityBuilder().payload(cnt).build();
    }

    @GetMapping("/select-by-project-id/{id}")
    public ResultEntity selectByProjectId(@PathVariable int id) {
        return resultEntityBuilder().payload(service.selectByProjectId(id)).build();
    }

    @PostMapping("/updateAll")
    public ResultEntity updateAll(@RequestBody List<ProjectResource> projectResources) {
        service.updateAll(projectResources);
        return resultEntityBuilder().build();
    }

    @GetMapping("/delete-by-project-id/{id}")
    public ResultEntity deleteByProjectId(@PathVariable int id) {
        return resultEntityBuilder().payload(service.deleteByProjectId(id)).build();
    }

    @GetMapping("/search")
    public ResultEntity getAll(String dsName,
                               String schemaName,
                               String tableName,
                               @RequestParam(defaultValue = "1") Integer pageNum,
                               @RequestParam(defaultValue = "10") Integer pageSize,
                               Integer projectId) {
        PageInfo<Map<String, Object>> page = service.queryResource(dsName, schemaName, tableName, pageNum, pageSize, projectId);
        return resultEntityBuilder().payload(page).build();
    }

    @GetMapping("/project-resource")
    public ResultEntity getProjectResource(String dsName,
                                           String schemaName,
                                           String tableName,
                                           @RequestParam(defaultValue = "1") Integer pageNum,
                                           @RequestParam(defaultValue = "10") Integer pageSize,
                                           @RequestParam Integer projectId) {
        PageInfo<Map<String, Object>> page = service.queryProjectResource(dsName, schemaName, tableName, pageNum, pageSize, projectId);
        return resultEntityBuilder().payload(page).build();
    }

    @GetMapping("/datasourceNames")
    public ResultEntity getDatasourceNames(@RequestParam(required = false) Integer projectId) {
        List<Map<String, Object>> dsNames = service.getDatasourceNames(projectId);
        return resultEntityBuilder().payload(dsNames).build();
    }

    @GetMapping("/project-names")
    public ResultEntity getProjectNames() {
        return resultEntityBuilder().payload(service.getProjectNames()).build();
    }

    @GetMapping("/{projectId}/{tableId}")
    public ResultEntity getResourceByPIdAndTId(@PathVariable Integer projectId, @PathVariable Integer tableId) {
        return resultEntityBuilder().payload(service.getByPIdAndTId(projectId, tableId)).build();
    }

    @GetMapping("/status/{projectId}/{tableId}")
    public ResultEntity checkResourceStatus(@PathVariable Integer projectId, @PathVariable Integer tableId) {
        return resultEntityBuilder().payload(service.checkStatus(projectId, tableId)).build();
    }


}
