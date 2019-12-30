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
import com.creditease.dbus.domain.model.DataTable;
import com.creditease.dbus.domain.model.ProjectTopoTable;
import com.creditease.dbus.service.FullPullService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * This is Description
 *
 * @author xiancangao
 * @date 2018/12/28
 */
@RestController
@RequestMapping("/fullpull")
public class FullPullController extends BaseController {

    @Autowired
    private FullPullService fullPullService;

    @PostMapping("/updateCondition")
    public ResultEntity updateFullpullCondition(@RequestBody ProjectTopoTable projectTopoTable) {
        try {
            return resultEntityBuilder().status(fullPullService.updateFullpullCondition(projectTopoTable)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while request updateFullpullCondition Table .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping("/updateSourceFullpullCondition")
    public ResultEntity updateSourceFullpullCondition(@RequestBody DataTable dataTable) {
        try {
            return resultEntityBuilder().status(fullPullService.updateSourceFullpullCondition(dataTable)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while request updateSourceFullpullCondition Table .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }


}
