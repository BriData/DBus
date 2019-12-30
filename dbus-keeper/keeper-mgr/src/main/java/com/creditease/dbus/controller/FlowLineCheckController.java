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
import com.creditease.dbus.service.FlowLineCheckService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * Created by Administrator on 2018/8/5.
 */
@RestController
@RequestMapping("/flow-line-check")
public class FlowLineCheckController extends BaseController {

    @Autowired
    private FlowLineCheckService service;

    @GetMapping("/check/{tableId}")
    public ResultEntity check(@PathVariable Integer tableId) {
        return service.checkFlowLine(tableId);
    }

    @GetMapping("/checkDataLine")
    public ResultEntity checkDataLine(@RequestParam String dsName, @RequestParam String schemaName) {
        try {
            return resultEntityBuilder().payload(service.checkDataLine(dsName, schemaName)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while checkDataLine ", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

}
