/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2018 Bridata
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

import java.io.IOException;
import java.util.Map;

import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;

import com.creditease.dbus.annotation.ProjectAuthority;
import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.domain.model.ProjectTopology;
import com.creditease.dbus.service.ProjectTopologyService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by mal on 2018/4/12.
 */
@RestController
@RequestMapping("/project-topos")
//@ServerEndpoint(value = "/project-topos-opt", configurator = EndpointConfigure.class)
public class ProjectTopologyController extends BaseController {

    @Autowired
    private ProjectTopologyService service;

    @Autowired
    private SimpMessagingTemplate smt;

    @PostMapping("/insert")
    public ResultEntity insert(@RequestBody ProjectTopology record) throws Exception {
        ResultEntity result = service.insert(record);
        if (!result.success())
            return resultEntityBuilder().status(result.getStatus()).build();
        return result;
    }

    @PostMapping("/update")
    public ResultEntity update(@RequestBody ProjectTopology record) throws Exception {
        return service.update(record, false);
    }

    @GetMapping("/exist/{topoName}")
    public ResultEntity existTopoName(@PathVariable String topoName) {
        return service.existTopoName(topoName);
    }

    @GetMapping("/select/{topoId}")
    public ResultEntity select(@PathVariable Integer topoId) {
        return service.select(topoId);
    }

    @GetMapping("/delete/{topoId}")
    public ResultEntity delete(@PathVariable Integer topoId) throws Exception {
        ResultEntity result = service.delete(topoId);
        if (!result.success())
            return resultEntityBuilder().status(result.getStatus()).build();
        return result;
    }

    @GetMapping("/versions")
    public ResultEntity queryJarVersions() {
        return service.queryJarVersions();
    }

    @GetMapping("/packages")
    public ResultEntity queryJarPackages(String version) {
        return service.queryJarPackages(version);
    }

    @GetMapping("/topos")
    @ProjectAuthority
    public ResultEntity queryTopos(Integer projectId,
                                   String topoName,
                                   Integer pageNum,
                                   Integer pageSize,
                                   String sortby,
                                   String order) throws Exception {
        return service.queryTopos(projectId, topoName, pageNum, pageSize, sortby, order);
    }

    @GetMapping("/out-topics/{projectId}/{topoId}")
    @ProjectAuthority
    public ResultEntity queryOutPutTopics(@PathVariable Integer projectId,
                                          @PathVariable Integer topoId) {
        return service.queryOutPutTopics(projectId, topoId);
    }

    @GetMapping("/in-topics/{projectId}/{topoId}")
    @ProjectAuthority
    public ResultEntity queryInPutTopics(@PathVariable Integer projectId,
                                         @PathVariable Integer topoId) {
        return service.queryInPutTopics(projectId, topoId);
    }

    @GetMapping("/template")
    public ResultEntity queryRouterTopologyConfigTemplate() {
        return service.obtainRouterTopologyConfigTemplate();
    }

    @GetMapping("/rerun-init/{projectId}/{topoId}")
    @ProjectAuthority
    public ResultEntity rerunInit(@PathVariable Integer projectId,
                                @PathVariable Integer topoId) throws Exception {
        return service.rerunInit(projectId, topoId);
    }

    @PostMapping("/rerun")
    public ResultEntity rerun(@RequestBody Map<String, String> map) {
        return service.rerun(map);
    }

    @GetMapping("/effect/{topoId}")
    public ResultEntity effect(@PathVariable Integer topoId) {
        return service.effect(topoId);
    }

    @PostMapping("/operate")
    public ResultEntity operate(@RequestBody Map<String, Object> map) throws Exception {
        return service.operate(null, null, map, null);
    }

    @MessageMapping("/startOrStop")
    public void startOrStop(Map<String, Object> map) throws Exception {
        service.operate(null, null, map, smt);
    }

    @OnOpen
    public void onOpen(Session session) {
        logger.info("create connection.");
    }

    @OnMessage
    public void onMessage(String msg, Session session) throws Exception {
        logger.info("send message.");
        service.operate(msg, session, null, null);
    }

    @OnError
    public void onError(Session session, Throwable error) {
        logger.info("made an error.");
    }

    @OnClose
    public void onClose(Session session) throws IOException {
        logger.info("close connection.");
        session.close();
    }

}
