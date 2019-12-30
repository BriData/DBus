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
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.domain.model.User;
import com.creditease.dbus.service.UserService;
import com.creditease.dbus.utils.DBusUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * Created by zhangyf on 2018/3/21.
 */
@RestController
@RequestMapping("/users")
public class UserController extends BaseController {
    @Autowired
    private UserService service;

    @PostMapping(path = "create", consumes = "application/json")
    public ResultEntity createUser(Boolean encoded, @RequestBody User user) {
        if (StringUtils.isBlank(user.getEmail())) {
            return resultEntityBuilder().status(MessageCode.MAILBOX_EMPTY).build();
        }
        if (encoded == null || !encoded) {
            user.setPassword(DBusUtils.md5(user.getPassword()));
        }
        return service.createUser(user);
    }

    @PostMapping(path = "update", consumes = "application/json")
    public ResultEntity updateUser(Boolean encoded, @RequestBody User user) {
        if (user.getId() == null) {
            return resultEntityBuilder().status(MessageCode.USER_ID_EMPTY).build();
        }
        if (encoded == null || !encoded) {
            user.setPassword(DBusUtils.md5(user.getPassword()));
        }
        return service.updateUser(user);
    }

    @GetMapping(path = "delete/{id}")
    public ResultEntity deleteUser(@PathVariable Integer id) {
        return service.deleteUser(id);
    }

    @GetMapping(path = "{id}")
    public ResultEntity getUser(@PathVariable int id) {
        return service.getUser(id);
    }

    @GetMapping(path = "search")
    public ResultEntity search(String email,
                               String phoneNum,
                               String userName,
                               String status,
                               Integer pageNum,
                               Integer pageSize,
                               String sortby,
                               String order) {
        User user = new User();
        user.setUserName(userName);
        user.setPhoneNum(phoneNum);
        user.setEmail(email);
        user.setStatus(status);
        return service.search(user, pageNum, pageSize, sortby, order);
    }

    @GetMapping(path = "modifyPassword")
    public ResultEntity modifyPassword(Boolean encoded, @RequestParam String oldPassword, @RequestParam String newPassword,
                                       @RequestParam Integer id) {
        if (oldPassword == null) {
            return resultEntityBuilder().status(MessageCode.OLD_PASSWORD_EMPTY).build();
        }
        if (newPassword == null) {
            return resultEntityBuilder().status(MessageCode.NEW_PASSWORD_EMPTY).build();
        }
        if (id == null) {
            return resultEntityBuilder().status(MessageCode.USER_ID_EMPTY).build();
        }
        if (encoded == null || !encoded) {
            oldPassword = DBusUtils.md5(oldPassword);
            newPassword = DBusUtils.md5(newPassword);
        }
        return service.modifyPassword(oldPassword, newPassword, id);
    }
}
