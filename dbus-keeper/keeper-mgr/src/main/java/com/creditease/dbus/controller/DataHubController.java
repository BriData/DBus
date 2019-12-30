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
import com.creditease.dbus.service.DataHubService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/datahub")
public class DataHubController extends BaseController {

    @Autowired
    private DataHubService dataHubService;

    @GetMapping("/startTable")
    public ResultEntity startTable(@RequestParam Long schemaId, @RequestParam String tableName) {
        try {
            dataHubService.startTable(schemaId, tableName);
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while startTable ", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping("/createTopic")
    public ResultEntity createTopic(@RequestParam String topic, @RequestParam String user) {
        try {
            dataHubService.createTopic(topic, user);
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while startTable ", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping("/getAllDataSourceInfo")
    public ResultEntity getAllDataSourceInfo(String dsName) {
        try {
            logger.info("收到获取数据源详情接口.dsName:{}", dsName);
            return dataHubService.getAllDataSourceInfo(dsName);
        } catch (Exception e) {
            logger.error("Exception encountered while getAllDataSourceInfo ", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

}
