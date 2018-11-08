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

import com.creditease.dbus.annotation.AdminPrivilege;
import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.domain.model.FullPullHistory;
import com.creditease.dbus.service.FullPullHistoryService;
import com.creditease.dbus.service.ProjectResourceService;
import org.apache.catalina.servlet4preview.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.net.URLDecoder;

/**
 * Created by xiancangao on 2018/04/16.
 */
@RestController
@RequestMapping("/fullpullHistory")
@AdminPrivilege
public class FullPullHistoryController extends BaseController {
    @Autowired
    private FullPullHistoryService service;
    @Autowired
    private ProjectResourceService resService;
    @GetMapping("/search")
    public ResultEntity search(HttpServletRequest request) throws Exception{
        return service.search(URLDecoder.decode(request.getQueryString(),"UTF-8"));
    }

    @GetMapping("/project-names")
    public ResultEntity queryProjectFullpullHistory(HttpServletRequest request) throws Exception{
        return service.queryProjectNames(request.getQueryString());
    }

    @GetMapping("/dsNames")
    public ResultEntity getDSNames(HttpServletRequest request) throws Exception{
        return service.getDSNames(request.getQueryString());
    }

    @PostMapping("/dsNames")
    public ResultEntity updata(HttpServletRequest request) throws Exception{
        return service.getDSNames(request.getQueryString());
    }
    @PostMapping("/update")
    public ResultEntity update(@RequestBody FullPullHistory fullPullHistory) throws Exception{
        return service.update(fullPullHistory);
    }

    @GetMapping("/clearFullPullAlarm")
    public ResultEntity clearFullPullAlarm(@RequestParam String dsName) throws Exception {
        service.clearFullPullAlarm(dsName);
        return resultEntityBuilder().build();
    }
}
