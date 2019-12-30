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

import com.creditease.dbus.annotation.AdminPrivilege;
import com.creditease.dbus.annotation.ProjectAuthority;
import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.service.ProjectResourceService;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.net.URLDecoder;

/**
 * Created with IntelliJ IDEA
 * Description:
 * User: 王少楠
 * Date: 2018-04-18
 * Time: 下午6:27
 */
@RestController
@RequestMapping("/projectResource")
public class ProjectResourceController extends BaseController {

    @Autowired
    private ProjectResourceService service;

    @GetMapping("/resources")
    @AdminPrivilege
    public ResultEntity queryResource(HttpServletRequest request) throws Exception {
        return service.queryResources(URLDecoder.decode(request.getQueryString(), "UTF-8"));
    }

    @GetMapping("/project-resource")
    @ProjectAuthority
    public ResultEntity queryProjectResource(HttpServletRequest request) throws Exception {
        return service.queryProjectResources(URLDecoder.decode(request.getQueryString(), "UTF-8"));
    }

    @GetMapping("/mask")
    @ProjectAuthority
    public ResultEntity getEncodeHint(HttpServletRequest request) throws Exception {
        return service.getEncodeHint(URLDecoder.decode(request.getQueryString(), "UTF-8"));
    }

    @GetMapping("/dsNames")
    public ResultEntity getDSNames(HttpServletRequest request) throws Exception {
        String role = currentUserRole();
        if (StringUtils.equalsIgnoreCase(role, "admin")
                || StringUtils.isNotEmpty(request.getParameter("projectId"))) {
            return service.getDSNames(URLDecoder.decode(request.getQueryString(), "UTF-8"));
        } else {
            return resultEntityBuilder().status(MessageCode.PROJECT_ID_EMPTY).build();
        }

    }

    @ApiOperation(value = "get project names", notes = "resouces 下的所有可查询的project")
    @GetMapping("/project-names")
    @AdminPrivilege
    public ResultEntity getProjectNames() {
        return service.queryProjectNames();
    }
}
