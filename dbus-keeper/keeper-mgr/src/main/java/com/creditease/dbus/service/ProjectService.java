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

package com.creditease.dbus.service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.base.com.creditease.dbus.utils.RequestSender;
import com.creditease.dbus.bean.ProjectBean;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.constant.ServiceNames;
import com.creditease.dbus.domain.model.Project;
import com.creditease.dbus.domain.model.ProjectEncodeHint;
import com.creditease.dbus.domain.model.ProjectResource;
import com.creditease.dbus.domain.model.ProjectSink;
import com.creditease.dbus.domain.model.ProjectTopoTable;
import com.creditease.dbus.domain.model.ProjectUser;
import com.creditease.dbus.domain.model.User;
import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

/**
 * Created by zhangyf on 2018/3/7.
 */
@Service
public class ProjectService {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private RequestSender sender;

    @Autowired
    private IZkService zkService;

    public ResultEntity queryProjects(Integer userId, String roleType) {
        Map<String, Object> param = new HashMap<>();
        if (!StringUtils.equals(KeeperConstants.USER_ROLE_TYPE_ADMIN, roleType)) {
            param.put("userId", userId);
            param.put("roleType", roleType);
        }
        ResponseEntity<ResultEntity> result = sender.post(ServiceNames.KEEPER_SERVICE, "/projects", param);
        return result.getBody();
    }

    public ResultEntity queryUserRelationProjects(Integer userId, String roleType) {
        Map<String, Object> param = new HashMap<>();
        param.put("userId", userId);
        param.put("roleType", roleType);
        ResponseEntity<ResultEntity> result = sender.post(ServiceNames.KEEPER_SERVICE, "/projects/user-relation-projects", param);
        return result.getBody();
    }

    public ResultEntity queryUsers(User user,
                                   Integer pageNum,
                                   Integer pageSize,
                                   String sortby,
                                   String order) {
        String url = "/users/search?pageNum=" + pageNum + "&pageSize=" + pageSize + "&sortby=" + sortby + "&order=" + order;
        ResponseEntity<ResultEntity> result = sender.post(ServiceNames.KEEPER_SERVICE, url, user);
        return result.getBody();
    }

    public ResultEntity queryEncoderRules() {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projects/encoders");
        return result.getBody();
    }

    public ResultEntity querySinks(String queryString) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/sinks/search", queryString);
        return result.getBody();
    }

    public ResultEntity queryResources(String queryString) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projects/resources", queryString);
        return result.getBody();
    }

    public ResultEntity queryColumns(String queryString) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projects/columns", queryString);
        return result.getBody();
    }

    public ResultEntity update(Project project) {
        project.setUpdateTime(new Date());
        ResponseEntity<ResultEntity> result = sender.post(ServiceNames.KEEPER_SERVICE, "/projects/update", project);
        return result.getBody();
    }

    public ResultEntity deleteProject(int id) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projects/select/{id}", id);
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
            return result.getBody();
        Project project = result.getBody().getPayload(new TypeReference<Project>() {});

        result = sender.get(ServiceNames.KEEPER_SERVICE, "/projects/delete/{id}", id);
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
            return result.getBody();

        // 删除grafana dashboard
        Properties props = null;
        try {
            props = zkService.getProperties(Constants.COMMON_ROOT + "/" + Constants.GLOBAL_PROPERTIES);
            String host = props.getProperty("grafana_url_dbus");
            if (StringUtils.endsWith(host, "/"))
                host = StringUtils.substringBeforeLast(host, "/");
            String token = props.getProperty("grafanaToken");
            String api = "/api/dashboards/db/";
            String url = host + api + project.getProjectName();

            List<Object> ret = this.send(url, "GET", "", token);
            if ((int) ret.get(0) == 200) {
                ret = this.send(url, "DELETE", "", token);
                if ((int) ret.get(0) == 200) {
                    logger.info("delete dashboard success, {}", (String) ret.get(1));
                } else if (((int) ret.get(0) == -1)) {
                    logger.error("call url:{} fail", url);
                } else {
                    logger.warn("call url:{} response msg:{}", url, (String) ret.get(1));
                }
            } else if (((int) ret.get(0) == -1)) {
                logger.error("call url:{} fail", url);
            } else {
                logger.warn("call url:{} response msg:{}", url, (String) ret.get(1));
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        //if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
        //    return result.getBody();
        //
        //result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectUser/delete-by-project-id/{id}", id);
        //if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
        //    return result.getBody();
        //
        //result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectSink/delete-by-project-id/{id}", id);
        //if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
        //    return result.getBody();
        //
        //result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectResource/delete-by-project-id/{id}", id);
        //if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
        //    return result.getBody();
        //
        //result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectEncodeHint/delete-by-project-id/{id}", id);
        return result.getBody();
    }

    public ResultEntity queryById(int id) {
        // 修改项目时获取相应信息
        Map<String, Object> retMap = new HashMap<>();
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projects/select/{id}", id);
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
            return result.getBody();
        retMap.put("project", result.getBody().getPayload(Project.class));

        result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectUser/select-by-project-id/{id}", id);
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
            return result.getBody();
        retMap.put("users", result.getBody().getPayload(new TypeReference<List<Map<String, Object>>>() {}));

        result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectSink/select-by-project-id/{id}", id);
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
            return result.getBody();
        retMap.put("sinks", result.getBody().getPayload(new TypeReference<List<Map<String, Object>>>() {}));

        result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectResource/select-by-project-id/{id}", id);
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
            return result.getBody();
        retMap.put("resources", result.getBody().getPayload(new TypeReference<List<Map<String, Object>>>() {}));

        result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectEncodeHint/select-by-project-id/{id}", id);
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
            return result.getBody();
        retMap.put("encodes", result.getBody().getPayload(new TypeReference<Map<Integer, List<Map<String, Object>>>>() {}));

        result.getBody().setPayload(retMap);
        return result.getBody();
    }

    public ResultEntity addProject(ProjectBean bean) {
        // 插入项目基本信息
        Project project = bean.getProject();
        project.setUpdateTime(new Date());
        ResponseEntity<ResultEntity> result = sender.post(ServiceNames.KEEPER_SERVICE, "/projects/insert", project);
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
            return result.getBody();
        int projectId = (int)result.getBody().getPayload();

        List<ProjectUser> users =  bean.getUsers();
        if (users != null) {
            for (ProjectUser user : users) {
                user.setProjectId(projectId);
                user.setUpdateTime(new Date());
            }
            result = sender.post(ServiceNames.KEEPER_SERVICE, "/projectUser/insertAll", users);
            if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
                return result.getBody();
        }

        List<ProjectSink> sinks = bean.getSinks();
        if (sinks != null) {
            for (ProjectSink sink : sinks) {
                sink.setProjectId(projectId);
                sink.setUpdateTime(new Date());
            }
            result = sender.post(ServiceNames.KEEPER_SERVICE, "/projectSink/insertAll", sinks);
            if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
                return result.getBody();
        }

        List<ProjectResource> resources = bean.getResources();
        if (resources != null) {
            for (ProjectResource resource : resources) {
                resource.setProjectId(projectId);
                resource.setUpdateTime(new Date());
            }
            result = sender.post(ServiceNames.KEEPER_SERVICE, "/projectResource/insertAll", resources);
            if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
                return result.getBody();
        }

        Map<Integer, List<ProjectEncodeHint>> encodes = bean.getEncodes();
        if (encodes != null) {
            List<ProjectEncodeHint> encodeHints = new ArrayList<>();
            for (Map.Entry<Integer, List<ProjectEncodeHint>> entry : encodes.entrySet()) {
                for (ProjectEncodeHint encodeHint : entry.getValue()) {
                    encodeHint.setProjectId(projectId);
                    encodeHint.setTableId(entry.getKey());
                    encodeHint.setUpdateTime(new Date());
                }
                encodeHints.addAll(entry.getValue());
            }
            result = sender.post(ServiceNames.KEEPER_SERVICE, "/projectEncodeHint/insertAll", encodeHints);
            if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
                return result.getBody();
        }
        return result.getBody();
    }

    public ResultEntity updateProject(ProjectBean bean) {
        Project project = bean.getProject();
        project.setUpdateTime(new Date());

        /*由于project-resource更新的时候，调用的也是该方法。
        *所以需要先判断要删除的resource（如果有删除的），是否正在使用。
        * */
        List<ProjectResource> resources = bean.getResources();
        //1.先获取所有旧的resource
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectResource/select-by-project-id/{id}", project.getId());
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
            return result.getBody();
        List<Map<String,Object>> oldResources =result.getBody().getPayload(new TypeReference<List<Map<String, Object>>>() {});
        //2.判断resource能否删除。
        for(Map<String,Object> oldResource: oldResources){
            int tableId = (int) oldResource.get("id");
            result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectResource/status/{projectId}/{tableId}", project.getId(),tableId);
            String oldResourceStatus = result.getBody().getPayload(new TypeReference<String>() {});
            //如果删除了所有resource或者只删除当前resource,并且当前resource正在使用
            if((resources == null || isDeleted(resources,project.getId(),tableId))
                    && StringUtils.equals(ProjectResource.STATUS_USE,oldResourceStatus)){
                result.getBody().setStatus(MessageCode.PROJECT_RESOURCE_IS_USING);
                return result.getBody();
            }
        }

        result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectResource/delete-by-project-id/{id}", project.getId());
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
            return result.getBody();
        if (resources != null) {
            for (ProjectResource resource : resources) {
                resource.setUpdateTime(new Date());
            }
            result = sender.post(ServiceNames.KEEPER_SERVICE, "/projectResource/insertAll", resources);
            if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
                return result.getBody();
        }


        result = sender.post(ServiceNames.KEEPER_SERVICE, "/projects/update", project);
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
            return result.getBody();

        List<ProjectUser> users =  bean.getUsers();
        result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectUser/delete-by-project-id/{id}", project.getId());
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
            return result.getBody();
        if (users != null) {
            for (ProjectUser user : users) {
                user.setUpdateTime(new Date());
            }
            result = sender.post(ServiceNames.KEEPER_SERVICE, "/projectUser/insertAll", users);
            if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
                return result.getBody();
        }

        List<ProjectSink> sinks = bean.getSinks();
        result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectSink/delete-by-project-id/{id}", project.getId());
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
            return result.getBody();
        if (sinks != null) {
            for (ProjectSink sink : sinks) {
                sink.setUpdateTime(new Date());
            }
            result = sender.post(ServiceNames.KEEPER_SERVICE, "/projectSink/insertAll", sinks);
            if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
                return result.getBody();
        }

        /*List<ProjectResource> resources = bean.getResources();
        result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectResource/delete-by-project-id/{id}", project.getId());
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
            return result.getBody();
        if (resources != null) {
            for (ProjectResource resource : resources) {
                resource.setUpdateTime(new Date());
            }
            result = sender.post(ServiceNames.KEEPER_SERVICE, "/projectResource/insertAll", resources);
            if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
                return result.getBody();
        }*/

        Map<Integer, List<ProjectEncodeHint>> encodes = bean.getEncodes();
        result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectEncodeHint/delete-by-project-id/{id}", project.getId());
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
            return result.getBody();
        if (encodes != null) {
            List<ProjectEncodeHint> encodeHints = new ArrayList<>();
            for (Map.Entry<Integer, List<ProjectEncodeHint>> entry : encodes.entrySet()) {
                for (ProjectEncodeHint encodeHint : entry.getValue()) {
                    encodeHint.setUpdateTime(new Date());
                }
                encodeHints.addAll(entry.getValue());
            }
            result = sender.post(ServiceNames.KEEPER_SERVICE, "/projectEncodeHint/insertAll", encodeHints);
            if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
                return result.getBody();
        }
        return result.getBody();
    }

    /**
     * 判断某resoure是否要删除
     * @return 要删除true | 不删除 false
     */
    private boolean isDeleted(List<ProjectResource> resources, int projectId,int tableId){
        //能找到，说明没删除
        for(ProjectResource resource:resources){
            if(resource.getProjectId().intValue() == projectId
                    && resource.getTableId().intValue() ==tableId){
                return false;
            }
        }
        return true;
    }

    public ResultEntity getPrincipal(int id) {
        return sender.get(ServiceNames.KEEPER_SERVICE, "/projects/getPrincipal/{0}", id).getBody();
    }

    public ResultEntity getMountedProjrct(String  queryString) {
        return sender.get(ServiceNames.KEEPER_SERVICE, "/projects/getMountedProjrct", queryString).getBody();
    }

    /**
     * 根据projectId获取project信息，只有project的信息
     * @param projectId
     * @return Project
     */
    public ResultEntity queryProjectId(int projectId) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projects/select/{id}", projectId);
        return result.getBody();
    }

    public ResultEntity search(String queryString) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projects/search", queryString);
        return result.getBody();
    }

    public int getRunningTopoTables(int id) {
        List<ProjectTopoTable> projectTopoTables = sender.get(ServiceNames.KEEPER_SERVICE, "/projects/getRunningTopoTables/{0}", id).getBody().getPayload(new TypeReference<List<ProjectTopoTable>>() {
        });
        return projectTopoTables.size();
    }

    private List<Object> send(String serverUrl, String method, String param, String token) {
        List<Object> ret = new ArrayList<>();
        ret.add(-1);

        StringBuilder response = new StringBuilder();
        BufferedReader reader = null;
        BufferedWriter writer = null;
        URL url = null;
        try {
            url = new URL(serverUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestProperty("Content-Type", "application/json;charset=UTF-8");
            conn.setRequestProperty("Authorization", "Bearer " + token);
            conn.setRequestMethod(method);
            conn.setDoInput(true);
            conn.setConnectTimeout(1000 * 5);

            if (StringUtils.isNotBlank(param)) {
                conn.setDoOutput(true);
                writer = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream()));
                writer.write(param);
                writer.flush();
            }

            int httpStatus = conn.getResponseCode();
            ret.set(0, httpStatus);

            if (httpStatus == 401 ||
                    httpStatus == 403 ||
                    httpStatus == 404) {
                if (httpStatus == 404) {
                    reader = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
                } else {
                    response.append(conn.getResponseMessage());
                }
            } if (httpStatus == 200) {
                reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            }

            if (reader != null) {
                String line = null;
                while ((line = reader.readLine()) != null) {
                    response.append(line).append(SystemUtils.LINE_SEPARATOR);
                }
            }
        } catch (IOException e) {
            logger.error("send request error", e);
            ret.set(0, -1);
        } finally {
            IOUtils.closeQuietly(reader);
            IOUtils.closeQuietly(writer);
        }
        ret.add(response.toString());
        return ret;
    }

    public ResultEntity getAllResourcesByQuery(String queryString) {
        return sender.get(ServiceNames.KEEPER_SERVICE, "/projects/getAllResourcesByQuery", queryString).getBody();
    }
}
