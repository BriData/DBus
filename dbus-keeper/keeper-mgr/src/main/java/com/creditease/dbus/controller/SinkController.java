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
import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.domain.model.Sink;
import com.creditease.dbus.service.SinkService;
import org.apache.catalina.servlet4preview.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * Created by xiancangao on 2018/3/28.
 */
@RestController
@AdminPrivilege
@RequestMapping("/sinks")
public class SinkController extends BaseController {
    @Autowired
    private SinkService service;

    @PostMapping(path = "/create", consumes = "application/json")
    public ResultEntity createSink(@RequestBody Sink sink) {
        boolean b = service.sinkTest(sink.getUrl());
        if (!b) {
            return resultEntityBuilder().status(MessageCode.SINK_NEW_CONNECTIVITY_ERROR).build();
        }
        return service.createSink(sink);
    }

    @PostMapping(path = "/update", consumes = "application/json")
    public ResultEntity updateSink(@RequestBody Sink sink) {
        if (sink.getId() == null) {
            return resultEntityBuilder().status(MessageCode.SINK_ID_EMPTY).build();
        }
        boolean b = service.sinkTest(sink.getUrl());
        if (!b) {
            return resultEntityBuilder().status(MessageCode.SINK_NEW_CONNECTIVITY_ERROR).build();
        }
        return service.updateSink(sink);
    }

    @GetMapping("/delete/{id}")
    public ResultEntity deleteSink(@PathVariable Integer id) {
        if (id == null) {
            return resultEntityBuilder().status(MessageCode.SINK_ID_EMPTY).build();
        }
        return service.deleteSink(id);
    }

    @GetMapping(path = "/search")
    public ResultEntity search(HttpServletRequest request) {
        return service.search(request.getQueryString());
    }

    @GetMapping(path = "/search-by-user-project")
    public ResultEntity searchByUserProject(HttpServletRequest request) {
        return service.searchByUserProject(request.getQueryString());
    }

}
