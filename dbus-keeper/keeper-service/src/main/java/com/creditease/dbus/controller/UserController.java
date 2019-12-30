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
import com.creditease.dbus.domain.model.User;
import com.creditease.dbus.service.UserService;
import com.creditease.dbus.utils.DBusUtils;
import com.github.pagehelper.PageInfo;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.web.bind.annotation.*;

import java.util.Date;

@RestController
@RequestMapping(path = "/users")
public class UserController extends BaseController {
    @Autowired
    private UserService service;

    @ApiOperation(value = "create user")
    @ApiImplicitParam(name = "user", required = true, dataType = "User")
    @PostMapping(path = "create", consumes = "application/json")
    public ResultEntity createUser(@RequestBody User user) {
        if (StringUtils.isBlank(user.getEmail())) {
            return resultEntityBuilder().status(MessageCode.EMAIL_EMPTY).build();
        }
        try {
            user.setId(null); // 避免主键冲突
            user.setUpdateTime(new Date());
            service.createUser(user);
        } catch (DuplicateKeyException e) {
            return resultEntityBuilder().status(MessageCode.EMAIL_USED).build();
        }
        return resultEntityBuilder().payload(user).build();
    }

    @ApiOperation(value = "update user info", notes = "update user by user id")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "user id", required = true, dataType = "int"),
            @ApiImplicitParam(name = "user", value = "User object contains basic items", required = true, dataType = "User"),
    })
    @PostMapping(path = "update/{id}", consumes = "application/json")
    public ResultEntity update(@PathVariable Integer id, @RequestBody User user) {
        ResultEntityBuilder rb = resultEntityBuilder();
        if (StringUtils.isBlank(user.getEmail())) {
            return rb.status(MessageCode.EMAIL_EMPTY).build();
        }

        if (id == null) {
            return rb.status(MessageCode.USER_NOT_FOUND_OR_ID_EMPTY).build();
        }

        try {
            user.setUpdateTime(new Date());
            service.updateUser(user);
            return rb.build();
        } catch (DuplicateKeyException e) {
            return rb.status(MessageCode.EMAIL_USED).build();
        }
    }

    @ApiOperation(value = "delete user", notes = "delete user by path variable: id")
    @ApiImplicitParam(name = "id", value = "user id", required = true, dataType = "int")
    @GetMapping("delete/{id}")
    public ResultEntity delete(@PathVariable Integer id) {
        ResultEntityBuilder builder = resultEntityBuilder();
        if (id == null) {
            builder.status(MessageCode.USER_NOT_FOUND_OR_ID_EMPTY).build();
        } else {
            service.deleteUser(id);
        }
        return builder.build();
    }

    @ApiOperation(value = "get user", notes = "get user information by path variable: id")
    @ApiImplicitParam(name = "id", value = "user id", required = true, dataType = "int")
    @GetMapping("{id}")
    public ResultEntity getUser(@PathVariable Integer id) {
        ResultEntityBuilder rb = resultEntityBuilder();
        if (id == null) {
            rb.status(MessageCode.USER_NOT_FOUND_OR_ID_EMPTY).build();
        }

        User user = service.getUser(id);
        if (user == null) {
            return rb.status(MessageCode.USER_NOT_EXISTS).build();
        }
        return rb.payload(user).build();
    }

    @ApiOperation(value = "get by email", notes = "get user information by email")
    @ApiImplicitParam(name = "email", value = "email", required = true, dataType = "String")
    @GetMapping("/query-by-email")
    public ResultEntity queryByEmail(String email) {
        ResultEntityBuilder builder = resultEntityBuilder();
        if (StringUtils.isBlank(email)) {
            return builder.status(MessageCode.EMAIL_EMPTY).build();
        }

        User user = service.queryByEmail(email);
        return builder.payload(user).build();
    }

    @ApiOperation(value = "search users", notes = "search users by conditions")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "user", value = "User entity.", dataType = "User"),
            @ApiImplicitParam(name = "pageNum", value = "page num", dataType = "int"),
            @ApiImplicitParam(name = "pageSize", value = "page size", dataType = "int"),
            @ApiImplicitParam(name = "sortby", value = "sort by field", dataType = "String"),
            @ApiImplicitParam(name = "order", value = "order: asc/desc", dataType = "String")
    })
    @PostMapping(path = "search")
    public ResultEntity search(@RequestBody User user, Integer pageNum, Integer pageSize, String sortby, String order) {
        sortby = DBusUtils.underscoresNaming(sortby);
        if (!StringUtils.isBlank(order)) {
            if (!order.equalsIgnoreCase("asc") && !order.equalsIgnoreCase("desc")) {
                logger.warn("ignored invalid sort parameter[order]:{}", order);
                order = null;
            }
        }
        PageInfo<User> page = service.search(user, pageNum == null ? 1 : pageNum, pageSize == null ? 5 : pageSize, sortby, order);
        return resultEntityBuilder().payload(page).build();
    }

    @GetMapping(path = "modify-password")
    public ResultEntity modifyPassword(String oldPassword, String newPassword, Integer id) {
        int result = service.modifyPassword(oldPassword, newPassword, id);
        return resultEntityBuilder().status(result).build();
    }
}
