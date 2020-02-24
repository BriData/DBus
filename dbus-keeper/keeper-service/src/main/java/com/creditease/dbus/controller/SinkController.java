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
import com.creditease.dbus.domain.model.Sink;
import com.creditease.dbus.service.SinkService;
import com.creditease.dbus.utils.DBusUtils;
import com.github.pagehelper.PageInfo;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.web.bind.annotation.*;

import java.util.Date;

/**
 * Created by mal on 2018/3/23.
 */
@RestController
@RequestMapping("/sinks")
public class SinkController extends BaseController {

    @Autowired
    private SinkService service;

    @PostMapping(path = "/create", consumes = "application/json")
    public ResultEntity createSink(@RequestBody Sink sink) {
        ResultEntityBuilder reb = resultEntityBuilder();
        if (StringUtils.isBlank(sink.getSinkName())) {
            return reb.status(MessageCode.SINK_NAME_EMPTY).build();
        }
        if (StringUtils.isBlank(sink.getUrl())) {
            return reb.status(MessageCode.SINK_URL_EMPTY).build();
        }
        if (StringUtils.isBlank(sink.getSinkType())) {
            return reb.status(MessageCode.SINK_TYPE_EMPTY).build();
        }
        if (StringUtils.isBlank(sink.getSinkDesc())) {
            return reb.status(MessageCode.SINK_DESC_EMPTY).build();
        }
        //名称重复性校验
        Sink s = service.getSink(sink.getSinkName(), null);
        if (s != null) {
            return reb.status(MessageCode.SINK_NAME_USED).build();
        }
        try {
            sink.setId(null); // 避免主键冲突
            sink.setUpdateTime(new Date());
            sink.setIsGlobal((byte) 0);//默认0:false,1:true
            service.createSink(sink);
        } catch (DuplicateKeyException e) {
            return reb.status(MessageCode.SINK_NEW_EXCEPTION).build();
        }
        return reb.payload(sink).build();
    }

    @PostMapping(path = "/update", consumes = "application/json")
    public ResultEntity update(@RequestBody Sink sink) {
        ResultEntityBuilder reb = resultEntityBuilder();
        Sink s = service.getSink(sink.getSinkName(), sink.getId());
        if (s != null) {
            return reb.status(MessageCode.SINK_NAME_USED).build();
        }
        //该sink是否还有项目Topology在使用
        int count = service.getProjectBySinkId(sink.getId());
        if (count != 0) {
            Sink sinkDb = service.getSinkById(sink.getId());
            if (!StringUtils.equals(sink.getUrl(), sinkDb.getUrl()) || !StringUtils.equals(sink.getSinkType(), sinkDb.getSinkType())) {
                return reb.status(MessageCode.SINK_IS_USING).build();
            }
        }
        sink.setUpdateTime(new Date());
        service.updateSink(sink);
        return reb.build();
    }

    @GetMapping("/delete/{id}")
    public ResultEntity delete(@PathVariable Integer id) {
        ResultEntityBuilder reb = resultEntityBuilder();
        //该sink是否还有项目Topology在使用
        int count = service.getProjectBySinkId(id);
        if (count != 0) {
            return reb.status(MessageCode.SINK_IS_USING).build();
        }
        service.deleteSink(id);
        return reb.build();
    }

    @GetMapping("/search")
    public ResultEntity search(Sink sink, int pageNum, int pageSize, String sortby, String order) {
        sortby = DBusUtils.underscoresNaming(sortby);
        if (!StringUtils.isBlank(order)) {
            if (!order.equalsIgnoreCase("asc") && !order.equalsIgnoreCase("desc")) {
                logger.warn("ignored invalid sort parameter[order]:{}", order);
                order = null;
            }
        }
        PageInfo<Sink> page = service.search(sink, pageNum, pageSize, sortby, order);
        return resultEntityBuilder().payload(page).build();
    }

    @GetMapping("/search-by-user-project")
    public ResultEntity search(Integer pageNum, Integer pageSize, Integer userId, Integer projectId) {
        PageInfo<Sink> page = service.search(pageNum, pageSize, userId, projectId);
        return resultEntityBuilder().payload(page).build();
    }

    @GetMapping("/get-sink-by-id/{id}")
    public ResultEntity getSinkByID(@PathVariable Integer id) {
        Sink sink = service.getSinkById(id);
        ResultEntityBuilder reb = resultEntityBuilder();
        return reb.payload(sink).build();
    }

    @PostMapping(path = "/exampleSink", consumes = "application/json")
    public ResultEntity exampleSink(@RequestBody Sink sink) {
        service.exampleSink(sink);
        return resultEntityBuilder().build();
    }

}
