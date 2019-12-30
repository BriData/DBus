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
import com.creditease.dbus.service.ToolSetService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;


/**
 * Created by xiancangao on 2018/05/04.
 */
@RestController
@RequestMapping("/toolSet")
public class ToolSetController extends BaseController {
    @Autowired
    private ToolSetService toolSetService;

    @GetMapping("/source-table-column/{tableId}/{number}")
    public ResultEntity sourceTableColumn(@PathVariable Integer tableId, @PathVariable Integer number) {
        try {
            HashMap<String, Object> map = toolSetService.sourceTableColumn(tableId, number);
            return resultEntityBuilder().payload(map).build();
        } catch (Exception e) {
            logger.error("Exception encountered while send SourceTableColumn", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping("/getMgrDBMsg")
    public ResultEntity getMgrDBMsg() {
        try {
            HashMap<String, String> map = toolSetService.getMgrDBMsg();
            return resultEntityBuilder().payload(map).build();
        } catch (Exception e) {
            logger.error("Exception encountered while send SourceTableColumn", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping("/initMgrSql")
    public ResultEntity initMgrSql() {
        try {
            toolSetService.initMgrSql();
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while initMgrSql", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping("/getAllUniqueColumn")
    public ResultEntity getAllUniqueColumn() {
        try {
            HashMap<String, String> list = toolSetService.getAllUniqueColumn();
            return resultEntityBuilder().payload(list).build();
        } catch (Exception e) {
            logger.error("Exception encountered while getAllUniqueColumn", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

}
