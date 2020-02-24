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
import com.creditease.dbus.domain.model.SinkerTopology;
import com.creditease.dbus.service.SinkerService;
import com.creditease.dbus.utils.DBusUtils;
import com.github.pagehelper.PageInfo;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * Created by mal on 2018/3/23.
 */
@RestController
@RequestMapping("/sinker")
public class SinkerController extends BaseController {

    @Autowired
    private SinkerService service;

    @GetMapping("/search")
    public ResultEntity search(int pageNum, int pageSize, String sinkerName, String sortby, String order) {
        sortby = DBusUtils.underscoresNaming(sortby);
        if (!StringUtils.isBlank(order)) {
            if (!order.equalsIgnoreCase("asc") && !order.equalsIgnoreCase("desc")) {
                logger.warn("ignored invalid sort parameter[order]:{}", order);
                order = null;
            }
        }
        PageInfo<SinkerTopology> page = service.search(pageNum, pageSize, sinkerName, sortby, order);
        return resultEntityBuilder().payload(page).build();
    }

    @PostMapping(path = "/create")
    public ResultEntity create(@RequestBody SinkerTopology sinkerTopology) {
        try {
            SinkerTopology topology = service.searchBySinkerName(sinkerTopology.getSinkerName());
            if (topology != null) {
                return new ResultEntity(MessageCode.EXCEPTION, "sinker名称已存在");
            }
            service.create(sinkerTopology);
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while create sinker topology", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping(path = "/update")
    public ResultEntity update(@RequestBody SinkerTopology sinkerTopology) {
        try {
            service.update(sinkerTopology);
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while update sinker topology", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping(path = "/delete/{id}")
    public ResultEntity delete(@PathVariable Integer id) {
        try {
            service.delete(id);
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while delete sinker topology", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping(path = "/searchById/{id}")
    public ResultEntity searchSinkerTopologyById(@PathVariable Integer id) {
        try {
            return resultEntityBuilder().payload(service.searchById(id)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while get sinker topology by id", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

}
