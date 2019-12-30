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


package com.creditease.dbus.service;

import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.base.com.creditease.dbus.utils.RequestSender;
import com.creditease.dbus.base.com.creditease.dbus.utils.URLBuilder;
import com.creditease.dbus.constant.ServiceNames;
import com.creditease.dbus.domain.model.Project;
import com.creditease.dbus.domain.model.TopologyJar;
import org.apache.commons.lang.SystemUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by mal on 2018/4/19.
 */
@Service
public class JarManagerService {

    @Autowired
    private RequestSender sender;

    @Autowired
    private RestTemplate rest;

    private Logger logger = LoggerFactory.getLogger(getClass());

    public ResultEntity uploads(String category, String version, String type, MultipartFile jarFile) throws IOException {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);
        headers.setContentDispositionFormData("jarFile", jarFile.getOriginalFilename());

        MultiValueMap<String, Object> data = new LinkedMultiValueMap<>();
        File saveDir = new File(SystemUtils.getJavaIoTmpDir(), String.valueOf(System.currentTimeMillis()));
        if (!saveDir.exists()) saveDir.mkdirs();
        File tempFile = new File(saveDir, jarFile.getOriginalFilename());
        jarFile.transferTo(tempFile);
        FileSystemResource fsr = new FileSystemResource(tempFile);
        data.add("jarFile", fsr);

        HttpEntity<MultiValueMap<String, Object>> entity = new HttpEntity<>(data, headers);
        URLBuilder urlBulider = new URLBuilder(ServiceNames.KEEPER_SERVICE, "/jars/uploads/{0}/{1}/{2}");
        ResponseEntity<ResultEntity> result = rest.postForEntity(urlBulider.build(), entity, ResultEntity.class, version, type, category);
        if (tempFile.exists()) tempFile.delete();
        if (saveDir.exists()) saveDir.delete();

        return result.getBody();
    }

    public ResultEntity delete(Integer id) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/jars/delete/{0}", StringUtils.EMPTY, id);
        return result.getBody();
    }

    public ResultEntity queryVersion(String category) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/jars/versions/{0}", StringUtils.EMPTY, category);
        return result.getBody();
    }

    public ResultEntity queryType(String queryString) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/jars/types", queryString);
        return result.getBody();
    }

    public ResultEntity batchDelete(List<Integer> ids) {
        ResponseEntity<ResultEntity> result = sender.post(ServiceNames.KEEPER_SERVICE, "/jars/batch-delete", ids);
        return result.getBody();
    }

    public ResultEntity queryJarInfos(String queryString) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/jars/infos", queryString);
        return result.getBody();
    }

    public ResultEntity syncJarInfos() {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/jars/syncJarInfos");
        return result.getBody();
    }

    public ResultEntity updateJar(TopologyJar jar) {
        ResponseEntity<ResultEntity> result = sender.post(ServiceNames.KEEPER_SERVICE, "/jars/update", jar);
        return result.getBody();
    }

    public ResultEntity uploadsEncodePlugin(String name, Integer projectId, MultipartFile jarFile) throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);
        headers.setContentDispositionFormData("jarFile", jarFile.getOriginalFilename());

        MultiValueMap<String, Object> data = new LinkedMultiValueMap<>();
        File saveDir = new File(SystemUtils.getJavaIoTmpDir(), String.valueOf(System.currentTimeMillis()));
        if (!saveDir.exists()) saveDir.mkdirs();
        File tempFile = new File(saveDir, jarFile.getOriginalFilename());
        jarFile.transferTo(tempFile);
        FileSystemResource fsr = new FileSystemResource(tempFile);
        data.add("jarFile", fsr);

        HttpEntity<MultiValueMap<String, Object>> entity = new HttpEntity<>(data, headers);
        URLBuilder urlBulider = new URLBuilder(ServiceNames.KEEPER_SERVICE, "/jars/uploads-encode-plugin/{0}/{1}");
        ResponseEntity<ResultEntity> result = rest.postForEntity(urlBulider.build(), entity, ResultEntity.class, name, projectId);
        if (tempFile.exists()) tempFile.delete();
        if (saveDir.exists()) saveDir.delete();
        return result.getBody();
    }

    public ResultEntity uploadsKeytab(Integer projectId, String principal, MultipartFile jarFile) throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);
        headers.setContentDispositionFormData("jarFile", jarFile.getOriginalFilename());

        MultiValueMap<String, Object> data = new LinkedMultiValueMap<>();
        File saveDir = new File(SystemUtils.getJavaIoTmpDir(), String.valueOf(System.currentTimeMillis()));
        if (!saveDir.exists()) saveDir.mkdirs();
        File tempFile = new File(saveDir, jarFile.getOriginalFilename());
        jarFile.transferTo(tempFile);
        FileSystemResource fsr = new FileSystemResource(tempFile);
        data.add("jarFile", fsr);

        HttpEntity<MultiValueMap<String, Object>> entity = new HttpEntity<>(data, headers);
        URLBuilder urlBulider = new URLBuilder(ServiceNames.KEEPER_SERVICE, "/jars/uploads-keytab?projectId={0}&principal={1}");
        ResponseEntity<ResultEntity> result = rest.postForEntity(urlBulider.build(), entity, ResultEntity.class, projectId, principal);

        if (tempFile.exists()) tempFile.delete();
        if (saveDir.exists()) saveDir.delete();
        return result.getBody();
    }

    public ResponseEntity downloadKeytab(Integer projectId) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projects/select/{0}", projectId);
        Project project = result.getBody().getPayload(Project.class);
        String fileName = project.getKeytabPath();
        String projectName = project.getProjectName();
        if (StringUtils.isBlank(fileName)) {
            return ResponseEntity.ok().body(new ResultEntity(21003, "请联系管理员上传密钥文件"));
        }
        File file = new File(fileName);
        if (file == null) {
            return ResponseEntity.ok().body(new ResultEntity(21003, "请联系管理员上传密钥文件"));
        }
        HttpHeaders headers = new HttpHeaders();
        headers.add("Cache-Control", "no-cache, no-store, must-revalidate");
        headers.add("Content-Disposition", "attachment; filename=" + projectName + ".keytab");
        headers.add("Pragma", "no-cache");
        headers.add("Expires", "0");
        return ResponseEntity
                .ok()
                .headers(headers)
                .contentLength(file.length())
                .contentType(MediaType.parseMediaType("application/octet-stream"))
                .body(new FileSystemResource(file));
    }

    public ResultEntity searchEncodePlugin(String queryString) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/jars/search-encode-plugin", queryString);
        return result.getBody();
    }

    public ResultEntity deleteEncodePlugin(Integer id) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/jars/delete-encode-plugin/{0}", id);
        return result.getBody();
    }
}
