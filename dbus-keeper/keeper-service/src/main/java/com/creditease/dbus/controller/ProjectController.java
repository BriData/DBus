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

import java.util.Map;

import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.domain.model.Project;
import com.creditease.dbus.service.ProjectService;
import com.creditease.dbus.utils.DBusUtils;
import com.github.pagehelper.PageInfo;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by mal on 2018/3/21.
 */
@RestController
@RequestMapping("/projects")
public class ProjectController extends BaseController {

    @Autowired
    private ProjectService service;

    @GetMapping("/resources")
    public ResultEntity queryResources(String dsName,
                                  String schemaName,
                                  String tableName,
                                  int pageNum,
                                  int pageSize,
                                  String sortby,
                                  String order) {
        sortby = DBusUtils.underscoresNaming(sortby);
        if (!StringUtils.isBlank(order)) {
            if (!order.equalsIgnoreCase("asc") && !order.equalsIgnoreCase("desc")) {
                logger.warn("ignored invalid sort parameter[order]:{}", order);
                order = null;
            }
        }
        PageInfo<Map<String, Object>> page = service.queryResources(dsName, schemaName, tableName, pageNum, pageSize, sortby, order);
        return resultEntityBuilder().payload(page).build();
    }

    @GetMapping("/columns")
    public ResultEntity queryColumns(Integer tableId) {
        return resultEntityBuilder().payload(service.queryColumns(tableId)).build();
    }

    @PostMapping("/insert")
    public ResultEntity insert(@RequestBody Project project) {
        int id = service.insert(project);
        return resultEntityBuilder().payload(id).build();
    }

    @GetMapping("/select/{id}")
    public ResultEntity select(@PathVariable int id) {
        return resultEntityBuilder().payload(service.select(id)).build();
    }

    @PostMapping("/update")
    public ResultEntity update(@RequestBody Project project) {
        int id = service.update(project);
        return resultEntityBuilder().payload(id).build();
    }

    @GetMapping("/delete/{id}")
    public ResultEntity delete(@PathVariable int id) {
        return resultEntityBuilder().payload(service.delete(id)).build();
    }

    @PostMapping("")
    public ResultEntity projects(@RequestBody Map<String, Object> param) {
        return resultEntityBuilder().payload(service.queryProjects(param)).build();
    }

    @PostMapping("/user-relation-projects")
    public ResultEntity queryUserRelationProjects(@RequestBody Map<String, Object> param) {
        return resultEntityBuilder().payload(service.queryUserRelationProjects(param)).build();
    }

    @GetMapping("/encoders")
    public ResultEntity queryEncoderRules() throws Exception {
        return resultEntityBuilder().payload(service.queryEncoderRules()).build();
    }

    @GetMapping("/getPrincipal/{id}")
    public ResultEntity getPrincipal(@PathVariable("id") int id){
        return resultEntityBuilder().payload(service.getPrincipal(id)).build();
    }

    @GetMapping("/getMountedProjrct")
    public ResultEntity getMountedProjrct(Integer dsId, Integer tableId, Integer sinkId) {
        return resultEntityBuilder().payload(service.getMountedProjrct(dsId, tableId, sinkId)).build();
    }

    @GetMapping("/search")
    public ResultEntity search(int pageNum, int pageSize, String sortby, String order) {
        sortby = DBusUtils.underscoresNaming(sortby);
        if (!StringUtils.isBlank(order)) {
            if (!order.equalsIgnoreCase("asc") && !order.equalsIgnoreCase("desc")) {
                logger.warn("ignored invalid sort parameter[order]:{}", order);
                order = null;
            }
        }
        PageInfo<Project> page = service.search( pageNum, pageSize, sortby, order);
        return resultEntityBuilder().payload(page).build();
    }

}
