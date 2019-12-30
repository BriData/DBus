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
import com.creditease.dbus.domain.model.SinkerTopology;
import com.creditease.dbus.service.SinkService;
import org.apache.catalina.servlet4preview.http.HttpServletRequest;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.net.URLDecoder;
import java.util.Map;

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

    @GetMapping(path = "/searchSinkerTopology")
    public ResultEntity searchSinkerTopology(HttpServletRequest request) {
        try {
            return service.searchSinkerTopology(request.getQueryString());
        } catch (Exception e) {
            logger.error("Exception encountered while search sinker topology", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping(path = "/startOrStopSinkerTopology")
    public ResultEntity startOrStopSinkerTopology(@RequestBody Map<String, Object> param) {
        try {
            if (StringUtils.equals("start", (String) param.get("cmdType"))) {
                return resultEntityBuilder().payload(service.startSinkerTopology(param)).build();
            }
            if (StringUtils.equals("stop", (String) param.get("cmdType"))) {
                return service.stopSinkerTopology(param);
            }
            return new ResultEntity(MessageCode.EXCEPTION, "不支持的命令");
        } catch (Exception e) {
            logger.error("Exception encountered while start or stop sinker", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping(path = "/createSinkerTopology")
    public ResultEntity createSinkerTopology(@RequestBody SinkerTopology sinkerTopology) {
        try {
            return service.createSinkerTopology(sinkerTopology);
        } catch (Exception e) {
            logger.error("Exception encountered while create sinker topology", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping(path = "/updateSinkerTopology")
    public ResultEntity updateSinkerTopology(@RequestBody SinkerTopology sinkerTopology) {
        try {
            return service.updateSinkerTopology(sinkerTopology);
        } catch (Exception e) {
            logger.error("Exception encountered while update sinker topology", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping(path = "/deleteSinkerTopology/{id}")
    public ResultEntity deleteSinkerTopology(@PathVariable Integer id) {
        try {
            return service.deleteSinkerTopology(id);
        } catch (Exception e) {
            logger.error("Exception encountered while delete sinker topology", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping(path = "/reloadSinkerTopology")
    public ResultEntity reloadSinkerTopology(@RequestBody SinkerTopology sinkerTopology) {
        try {
            service.reloadSinkerTopology(sinkerTopology);
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while reload sinker topology", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping("/getSinkerTopicInfos")
    public ResultEntity getSinkerTopicInfos(@RequestParam String sinkerName) {
        try {
            return resultEntityBuilder().payload(service.getSinkerTopicInfos(sinkerName)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while get sinker topic infos", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping(path = "/dragBackRunAgain")
    public ResultEntity dragBackRunAgain(@RequestBody Map<String, Object> param) {
        try {
            service.dragBackRunAgain(param);
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while drag back run again", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping(path = "/searchSinkerTopologySchema")
    public ResultEntity searchSinkerTopologySchema(HttpServletRequest request) {
        try {
            return service.searchSinkerTopologySchema(URLDecoder.decode(request.getQueryString(), "UTF-8"));
        } catch (Exception e) {
            logger.error("Exception encountered while get sinker topology schema", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping(path = "/updateSinkerTopologySchema")
    public ResultEntity updateSinkerTopologySchema(@RequestBody Map<String, Object> param) {
        try {
            service.updateSinkerTopologySchema(param);
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while update sinker topology schema", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

}
