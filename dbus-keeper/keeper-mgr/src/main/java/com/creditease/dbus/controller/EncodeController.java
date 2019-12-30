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
import com.creditease.dbus.service.EncodeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/encode")
public class EncodeController extends BaseController {

    @Autowired
    private EncodeService service;

    /**
     * 脱敏查询
     * 参数 Integer pageNum, Integer pageSize, Integer projectId, Integer topoId, Integer dsId
     * String schemaName ,String tableName
     *
     * @param request
     * @return
     * @throws Exception
     */
    @GetMapping(path = "/search")
    public ResultEntity searchEncodeColumns(HttpServletRequest request) {
        return service.searchEncodeColumns(request.getQueryString());
    }

}
