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
import com.creditease.dbus.base.ResultEntityBuilder;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.service.AuthenticationService;
import com.creditease.dbus.utils.DBusUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Created by zhangyf on 2018/3/9.
 */
@RestController
@RequestMapping(value = "/auth/")
public class AuthenticationController extends BaseController {
    @Autowired
    private AuthenticationService service;

    @PostMapping(path = "/login", consumes = "application/json")
    public ResultEntity login(Boolean encoded, @RequestBody Map<String, String> req) {
        ResultEntityBuilder builder = resultEntityBuilder();

        String email = req.get("email");
        String password = req.get("password");
        if (StringUtils.isBlank(email)) {
            return builder.status(MessageCode.USER_NAME_EMPTY).build();
        }

        if (StringUtils.isBlank(password)) {
            return builder.status(MessageCode.PASSWORD_EMPTY).build();
        }
        if (encoded == null || !encoded) {
            password = DBusUtils.md5(password);
        }
        return service.doLogin(email, password);
    }
}
