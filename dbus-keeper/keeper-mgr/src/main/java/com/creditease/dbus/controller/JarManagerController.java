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

import com.creditease.dbus.annotation.ProjectAuthority;
import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.domain.model.TopologyJar;
import com.creditease.dbus.service.JarManagerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.net.URLDecoder;
import java.util.List;

/**
 * Created by mal on 2018/4/19.
 */
@RestController
@RequestMapping("/jars")
public class JarManagerController extends BaseController {

    @Autowired
    private JarManagerService service;

    @PostMapping("/uploads/{version}/{type}/{category}")
    public ResultEntity uploads(@PathVariable String version,
                                @PathVariable String type,
                                @PathVariable String category,
                                @RequestParam MultipartFile jarFile) throws Exception {
        return service.uploads(category, version, type, jarFile);
    }

    @GetMapping("/delete/{id}")
    public ResultEntity delete(@PathVariable Integer id) {
        return service.delete(id);
    }

    @GetMapping("/versions/{category}")
    public ResultEntity queryVersion(@PathVariable String category) {
        return service.queryVersion(category);
    }

    @GetMapping("/types")
    public ResultEntity queryType(HttpServletRequest req) throws Exception {
        return service.queryType(URLDecoder.decode(req.getQueryString(), "UTF-8"));
    }

    @PostMapping("/batch-delete")
    public ResultEntity batchDelete(@RequestBody List<Integer> ids) {
        return resultEntityBuilder().payload(service.batchDelete(ids)).build();
    }

    @GetMapping("/infos")
    public ResultEntity queryJarInfos(HttpServletRequest req) throws Exception {
        return service.queryJarInfos(URLDecoder.decode(req.getQueryString(), "UTF-8"));
    }

    @GetMapping("/sync")
    public ResultEntity syncJarInfos() throws Exception {
        return service.syncJarInfos();
    }

    @PostMapping("/update")
    public ResultEntity updateJar(@RequestBody TopologyJar jar) {
        return resultEntityBuilder().payload(service.updateJar(jar)).build();
    }

    /**
     * 脱敏插件上传
     *
     * @param name      插件名称
     * @param projectId 项目id(是否需要改成projectCode) TODO
     * @param jarFile   上传文件
     * @return
     * @throws Exception
     */
    @PostMapping(path = "/uploads-encode-plugin/{name}/{projectId}")
    @ProjectAuthority
    public ResultEntity uploadsEncodePlugin(@PathVariable String name, @PathVariable Integer projectId,
                                            @RequestParam MultipartFile jarFile) throws Exception {
        return service.uploadsEncodePlugin(name, projectId, jarFile);
    }

    /**
     * 脱敏插件分页查询,参数 pageNum, pageSize
     *
     * @param request
     * @return
     * @throws Exception
     */
    @GetMapping(path = "/search-encode-plugin")
    public ResultEntity searchEncodePlugin(HttpServletRequest request) throws Exception {
        return service.searchEncodePlugin(request.getQueryString());
    }

    /**
     * 脱敏插件删除
     *
     * @param id 插件id
     * @return
     * @throws Exception
     */
    @GetMapping(path = "/delete-encode-plugin/{id}")
    public ResultEntity deleteEncodePlugin(@PathVariable Integer id) {
        return service.deleteEncodePlugin(id);
    }

    @PostMapping(path = "/uploads-keytab/{projectId}/{principal}")
    @ProjectAuthority
    public ResultEntity uploadsKeytab(@PathVariable Integer projectId, @PathVariable String principal,
                                      @RequestParam MultipartFile jarFile) throws Exception {
        return service.uploadsKeytab(projectId, principal, jarFile);
    }

    @GetMapping("/download-keytab/{projectId}")
    @ProjectAuthority
    public ResponseEntity downloadKeytab(@PathVariable Integer projectId) {
        return service.downloadKeytab(projectId);
    }

}
