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
import com.creditease.dbus.domain.model.TableVersion;
import com.creditease.dbus.service.TableVersionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * Created by xiancangao on 2018/05/16.
 */
@RestController
@RequestMapping("/table-version")
public class TableVersionController extends BaseController {

    @Autowired
    private TableVersionService tableVersionService;

    @PostMapping(path = "update", consumes = "application/json")
    public ResultEntity updateDataTables(@RequestBody TableVersion tableVersion) {
        try {
            int id = tableVersionService.update(tableVersion);
            return resultEntityBuilder().payload(id).build();
        } catch (Exception e) {
            logger.error("Error encountered while update tableVersion .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping(path = "/get-by-table-id/{tableId}")
    public ResultEntity getVersionListByTableId(@PathVariable Integer tableId) {
        try {
            List<TableVersion> tableVersionsList = tableVersionService.getVersionListByTableId(tableId);
            return resultEntityBuilder().payload(tableVersionsList).build();
        } catch (Exception e) {
            logger.error("Error encountered while update tableVersion .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping(path = "get/{id}")
    public ResultEntity getById(@PathVariable Integer id) {
        try {
            TableVersion tableVersion = tableVersionService.getById(id);
            return resultEntityBuilder().payload(tableVersion).build();
        } catch (Exception e) {
            logger.error("Error encountered while update tableVersion .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }
}
