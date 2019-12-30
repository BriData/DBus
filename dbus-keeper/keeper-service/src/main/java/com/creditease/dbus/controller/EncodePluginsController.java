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
import com.creditease.dbus.domain.model.EncodePlugins;
import com.creditease.dbus.service.EncodePluginsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * User: 王少楠
 * Date: 2018-06-08
 * Time: 下午4:11
 */
@RestController
@RequestMapping("/encode-plugins")
public class EncodePluginsController extends BaseController {
    @Autowired
    private EncodePluginsService service;

    @GetMapping("/project-plugins/{projectId}")
    public ResultEntity getProjectEncodePlugins(@PathVariable Integer projectId) {
        List<EncodePlugins> projectEncodePlugins = service.getProjectEncodePlugins(projectId);
        return resultEntityBuilder().payload(projectEncodePlugins).build();
    }

    @PostMapping(path = "create", consumes = "application/json")
    public ResultEntity create(@RequestBody EncodePlugins encodePlugin) {
        service.create(encodePlugin);
        return resultEntityBuilder().build();
    }

}
