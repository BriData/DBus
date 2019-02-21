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

import java.net.URLDecoder;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import com.creditease.dbus.annotation.AdminPrivilege;
import com.creditease.dbus.annotation.ProjectAuthority;
import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.bean.ProjectBean;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.domain.model.Project;
import com.creditease.dbus.domain.model.Sink;
import com.creditease.dbus.domain.model.User;
import com.creditease.dbus.service.ProjectService;

import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.NumberUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static com.creditease.dbus.constant.MessageCode.PROJECT_NOT_ALLOWED_DELETED;

/**
 * Created by zhangyf on 2018/3/7.
 *
 */
@RestController
@RequestMapping("/projects")
public class ProjectController extends BaseController {

    @Autowired
    private ProjectService service;

    @ApiOperation(value="search projects", notes="search projects by userId, roleType")
    @ApiResponses({
            @ApiResponse(code = 200, message = "OK", response = Map[].class)
    })
    @GetMapping("")
    public ResultEntity queryProjects(HttpServletRequest req) {
        Integer userId = null;
        if (StringUtils.isNotBlank(req.getParameter("__user_id")))
            userId = NumberUtils.parseNumber(req.getParameter("__user_id"), Integer.class);
        String roleType = req.getParameter("__role_type");
        return service.queryProjects(userId, roleType);
    }

    @GetMapping("{userId}/{roleType}")
    public ResultEntity queryUserRelationProjects(@PathVariable Integer userId, @PathVariable String roleType) {
        return service.queryUserRelationProjects(userId, roleType);
    }

    @GetMapping("/encoders")
    public ResultEntity queryEncoderRules() {
        return service.queryEncoderRules();
    }

    @ApiOperation(value="search users", notes="search users by conditions")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "email", value = "email.", dataType = "String"),
            @ApiImplicitParam(name = "userName", value = "user name.", dataType = "String"),
            @ApiImplicitParam(name = "phoneNum", value = "phone num.", dataType = "String"),
            @ApiImplicitParam(name = "pageNum", value = "page num",  dataType = "int"),
            @ApiImplicitParam(name = "pageSize", value = "page size", dataType = "int"),
            @ApiImplicitParam(name = "sortby", value = "sort by field", dataType = "String"),
            @ApiImplicitParam(name = "order", value = "order: asc/desc", dataType = "String")
    })
    @ApiResponses({
            @ApiResponse(code = 200, message = "OK", response = User[].class)
    })
    @GetMapping("/users")
    public ResultEntity queryUsers(String email,
                                   String phoneNum,
                                   String userName,
                                   String status,
                                   Integer pageNum,
                                   Integer pageSize,
                                   String sortby,
                                   String order) throws Exception{
        User user = new User();
        user.setUserName(userName);
        user.setPhoneNum(phoneNum);
        user.setEmail(email);
        user.setStatus(status);
        return service.queryUsers(user, pageNum, pageSize, sortby, order);
    }

    @ApiOperation(value="search sinks", notes="search sinks by conditions")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "sinkName", value = "sink name.", dataType = "String"),
            @ApiImplicitParam(name = "url", value = "sink url.", dataType = "String"),
            @ApiImplicitParam(name = "pageNum", value = "page num",  dataType = "int"),
            @ApiImplicitParam(name = "pageSize", value = "page size", dataType = "int"),
            @ApiImplicitParam(name = "sortby", value = "sort by field", dataType = "String"),
            @ApiImplicitParam(name = "order", value = "order: asc/desc", dataType = "String")
    })
    @ApiResponses({
            @ApiResponse(code = 200, message = "OK", response = Sink[].class)
    })
    @GetMapping("/sinks")
    public ResultEntity querySinks(HttpServletRequest req) throws Exception {
        return service.querySinks(URLDecoder.decode(req.getQueryString(),"UTF-8"));
    }

    @ApiOperation(value="search resources", notes="search resources by conditions")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "dsName", value = "data source name.", dataType = "String"),
            @ApiImplicitParam(name = "schemaName", value = "schema name.", dataType = "String"),
            @ApiImplicitParam(name = "tableName", value = "table name.", dataType = "String"),
            @ApiImplicitParam(name = "pageNum", value = "page num",  dataType = "int"),
            @ApiImplicitParam(name = "pageSize", value = "page size", dataType = "int"),
            @ApiImplicitParam(name = "sortby", value = "sort by field", dataType = "String"),
            @ApiImplicitParam(name = "order", value = "order: asc/desc", dataType = "String")
    })
    @ApiResponses({
            @ApiResponse(code = 200, message = "OK", response = Map[].class)
    })
    @GetMapping("/resources")
    public ResultEntity queryResources(HttpServletRequest req) throws Exception {
        return service.queryResources(URLDecoder.decode(req.getQueryString(),"UTF-8"));
    }

    @ApiOperation(value="search Columns", notes="search Columns by conditions")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "tableId", value = "table id.", dataType = "int"),
    })
    @ApiResponses({
            @ApiResponse(code = 200, message = "OK", response = Map[].class)
    })
    @GetMapping("/columns")
    public ResultEntity queryColumns(HttpServletRequest req) throws Exception {
        return service.queryColumns(URLDecoder.decode(req.getQueryString(),"UTF-8"));
    }

    /**
     *
     *
     * @param bean
     *     {
     *      "project":{"id":1, "projectName":"test"},
     *      "users":[{"userId":1}, {"userId":2}],
     *      "sinks":[{"sinkId":1},{"sinkId":1}]，
     *      "resources":[{"tableId":1},{"tableId":2}],
     *      "encodes":{"1":[{"fieldName":"aaa"},{"fieldName":"bbb"}], "2":[{"fieldName":"ccc"}, {"fieldName":"ddd"}]}
     *     }
     * @return
     */
    @PostMapping(value = "/add", consumes = "application/json")
    @AdminPrivilege
    public ResultEntity addProject(@RequestBody ProjectBean bean) {
        return service.addProject(bean);
    }

    @ApiOperation(value="search project", notes="search project by project id")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "project id.", dataType = "int"),
    })
    @ApiResponses({
            @ApiResponse(code = 200, message = "OK", response = ProjectBean.class)
    })
    @GetMapping("{id}")
    @ProjectAuthority
    public ResultEntity queryById(@PathVariable("id") int id) {
        return service.queryById(id);
    }

    @PostMapping(value = "/update", consumes = "application/json")
    public ResultEntity updateProject(@RequestBody ProjectBean bean) {
        ResultEntity updateResult = service.updateProject(bean);
        //自定义返回的信息，需要在这里重新构造，才能读取i18n中的message信息。
        if(updateResult.getStatus() == MessageCode.PROJECT_RESOURCE_IS_USING){
            return resultEntityBuilder().status(MessageCode.PROJECT_RESOURCE_IS_USING).build();
        }else {
            return updateResult;
        }
    }

    @GetMapping("/delete/{id}")
    @ProjectAuthority
    public ResultEntity deleteById(@PathVariable("id") int id) {
        try {
            //判断能否直接删除project
            if(service.getRunningTopoTables(id) != 0){
                return resultEntityBuilder().status(PROJECT_NOT_ALLOWED_DELETED).build();
            }
            return service.deleteProject(id);
        } catch (Exception e) {
            logger.error("Exception when delete project", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping("/status")
    public ResultEntity toggleStatus(@RequestBody Project project) {
        return service.update(project);
    }

    @GetMapping("/getPrincipal/{id}")
    public ResultEntity getPrincipal(@PathVariable("id") int id){
        return service.getPrincipal(id);
    }

	/**
	 * 参数 Integer dsId, Integer tableId, Integer sinkId 任传其一
	 * @param request
	 * @return
	 */
	@GetMapping("/getMountedProjrct")
    public ResultEntity getMountedProjrct(HttpServletRequest request){
        return service.getMountedProjrct(request.getQueryString());
    }

    @GetMapping(path = "search")
    public ResultEntity search(HttpServletRequest request) {
        return service.search(request.getQueryString());
    }

    @GetMapping("/getAllResourcesByQuery")
    public ResultEntity getAllResourcesByQuery(HttpServletRequest request) throws Exception{
        return service.getAllResourcesByQuery(request.getQueryString());
    }

}
