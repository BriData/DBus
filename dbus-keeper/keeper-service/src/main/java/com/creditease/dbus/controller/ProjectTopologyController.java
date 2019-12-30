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
import com.creditease.dbus.domain.model.ProjectTopology;
import com.creditease.dbus.service.ProjectTopologyService;
import com.creditease.dbus.utils.DBusUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Created by mal on 2018/4/12.
 */
@RestController
@RequestMapping("/project-topos")
public class ProjectTopologyController extends BaseController {

    @Autowired
    private ProjectTopologyService service;

    @GetMapping("/select/{topoId}")
    public ResultEntity select(@PathVariable Integer topoId) {
        return resultEntityBuilder().payload(service.select(topoId)).build();
    }

    @GetMapping("/delete/{topoId}")
    public ResultEntity delete(@PathVariable Integer topoId) {
        return resultEntityBuilder().payload(service.delete(topoId)).build();
    }

    @PostMapping("/update")
    public ResultEntity update(@RequestBody ProjectTopology record) {
        int cnt = service.update(record);
        return resultEntityBuilder().payload(cnt).build();
    }

    @PostMapping("/insert")
    public ResultEntity insert(@RequestBody ProjectTopology record) {
        int id = service.insert(record);
        return resultEntityBuilder().payload(id).build();
    }

    @GetMapping("/exist/{topoName}")
    public ResultEntity existTopoName(@PathVariable String topoName) {
        int cnt = service.existTopoName(topoName);
        return resultEntityBuilder().payload(cnt).build();
    }

    @GetMapping("/versions")
    public ResultEntity queryJarVersions() throws Exception {
        return resultEntityBuilder().payload(service.queryJarVersions()).build();
    }

    @GetMapping("/packages")
    public ResultEntity queryJarPackages(String version) throws Exception {
        return resultEntityBuilder().payload(service.queryJarPackages(version)).build();
    }

    @GetMapping("/topos")
    public ResultEntity queryTopos(Integer projectId,
                                   String topoName,
                                   Integer pageNum,
                                   Integer pageSize,
                                   String sortby,
                                   String order) {
        sortby = DBusUtils.underscoresNaming(sortby);
        if (!StringUtils.isBlank(order)) {
            if (!order.equalsIgnoreCase("asc") && !order.equalsIgnoreCase("desc")) {
                logger.warn("ignored invalid sort parameter[order]:{}", order);
                order = null;
            }
        }
        return resultEntityBuilder().payload(service.search(projectId, topoName, pageNum, pageSize, sortby, order)).build();
    }

    @GetMapping("/out-topics/{projectId}/{topoId}")
    public ResultEntity queryOutPutTopics(@PathVariable Integer projectId,
                                          @PathVariable Integer topoId) {
        return resultEntityBuilder().payload(service.queryOutPutTopics(projectId, topoId)).build();
    }

    @GetMapping("/in-topics/{projectId}/{topoId}")
    public ResultEntity queryInPutTopics(@PathVariable Integer projectId,
                                         @PathVariable Integer topoId) {
        return resultEntityBuilder().payload(service.queryInPutTopics(projectId, topoId)).build();
    }

    @GetMapping("/template")
    public ResultEntity queryRouterTopologyConfigTemplate() {
        return resultEntityBuilder().payload(service.obtainRouterTopologyConfigTemplate()).build();
    }

    @GetMapping("/rerun-init/{projectId}/{topologyId}")
    public ResultEntity rerunInit(@PathVariable Integer projectId,
                                  @PathVariable Integer topologyId) {
        return resultEntityBuilder().payload(service.rerunInit(projectId, topologyId)).build();
    }

    @PostMapping("/rerun")
    public ResultEntity rerun(@RequestBody Map<String, String> map) {
        String topologyCode = map.get("topologyCode");
        String ctrlMsg = map.get("ctrlMsg");
        service.rerunTopology(topologyCode, ctrlMsg);
        return resultEntityBuilder().build();
    }

    @GetMapping("/effect/{topologyId}")
    public ResultEntity effect(@PathVariable Integer topologyId) {
        service.effectTopologyConfig(topologyId);
        return resultEntityBuilder().build();
    }

    @GetMapping("/getTopoAlias/{topoId}")
    public ResultEntity getTopoAlias(@PathVariable Integer topoId) {
        return resultEntityBuilder().payload(service.getTopoAlias(topoId)).build();
    }

    @PostMapping("/selectByIds")
    public ResultEntity selectByIds(@RequestBody List<Integer> ids) {
        return resultEntityBuilder().payload(service.selectByIds(ids)).build();
    }

}
