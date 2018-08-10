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

import com.creditease.dbus.annotation.AdminPrivilege;
import com.creditease.dbus.annotation.ProjectAuthority;
import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.bean.ProjectTableOffsetBean;
import com.creditease.dbus.bean.TableBean;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.service.ProjectTableService;
import io.swagger.annotations.*;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.net.URLDecoder;
import java.util.List;

/**
 * Created with IntelliJ IDEA
 * Description:
 * User: 王少楠
 * Date: 2018-04-18
 * Time: 下午6:31
 */
@RestController
@RequestMapping("/projectTable")
public class ProjectTableController extends BaseController {

    @Autowired
    private ProjectTableService service;

    @GetMapping("/tables")
    public ResultEntity queryTable(HttpServletRequest request) throws Exception {
        return service.queryTable(URLDecoder.decode(request.getQueryString(), "UTF-8"));
    }

    @ApiOperation(value = "getProjectTopoNames ", notes = "获取project下所有topology的id和name信息")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "projectId", value = "project id", dataType = "Integer")
    })
    @GetMapping("/topology-names")
    @ProjectAuthority
    public ResultEntity queryTopologyNames(HttpServletRequest request) throws Exception {
        return service.queryTopologyNames(request.getQueryString());
        /*if (request == null || request.getQueryString() == null)
            return service.queryTopologyNames(null);
        else
            return service.queryTopologyNames(URLDecoder.decode(request.getQueryString(), "UTF-8"));*/
    }

    @ApiOperation(value = "get project topologies", notes = "获取project下所有topo")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "projectId", value = "project id", dataType = "Integer")
    })
    @GetMapping("/project-topologies")
    @ProjectAuthority
    public ResultEntity getProjectTopologies(HttpServletRequest request) throws Exception {
        return service.getPojectTopologies(URLDecoder.decode(request.getQueryString(), "UTF-8"));
    }

    @GetMapping("/project-names")
    @AdminPrivilege
    public ResultEntity queryProjectNames() {
        return service.queryProjectNames();
    }

    @GetMapping("/datasource-names")
    @ProjectAuthority
    public ResultEntity queryDSNames(HttpServletRequest request) {
        String role = currentUserRole();
        if (StringUtils.equalsIgnoreCase(role, "admin")
                || StringUtils.isNotEmpty(request.getParameter("projectId"))) {
            return service.queryDSNames(request.getQueryString());
        }else {
            return resultEntityBuilder().status(MessageCode.PROJECT_ID_EMPTY).build();
        }

    }

    @ApiOperation(value = "getResroucesInProject",notes = "分页获取该project中某topology下可用的resource信息")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "dsName",value = "data source name",dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "schemaName",value = "schema 的 name",dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "tableName",value = "table name",dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "projectId",value = "当前项目的id",dataType = "Integer", paramType = "query",required = true),
            @ApiImplicitParam(name = "topoId",value = "topology的id",dataType = "Integer", paramType = "query",required = true),
            @ApiImplicitParam(name = "pageNum",value = "分页信息：页码",dataType = "Integer", paramType = "query",required = true),
            @ApiImplicitParam(name = "pageSize",value = "分页信息：page size",dataType = "Integer", paramType = "query",required = true)
    })
    @GetMapping("/project-resources")
    @ProjectAuthority
    public ResultEntity queryProjectResources(HttpServletRequest request) throws Exception {
        return service.queryProjectResources(URLDecoder.decode(request.getQueryString(), "UTF-8"));
    }

    @ApiOperation(value = "get columns", notes = "table下columns的信息，包括admin添加的脱敏信息")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "projectId", value = "project id", dataType = "Integer"),
            @ApiImplicitParam(name = "tableId", value = "table id", dataType = "Integer"),
    })
    @GetMapping("/columns")
    @ProjectAuthority
    public ResultEntity getColumns(HttpServletRequest request) throws Exception {
        if (request == null) {
            return resultEntityBuilder().status(MessageCode.PROJECT_ID_OR_TABLE_ID_EMPTY).build();
        }
        String rowQueryString = request.getQueryString();
        if (StringUtils.isEmpty(request.getParameter("projectId"))
                ||StringUtils.isEmpty(request.getParameter("tableId"))) {
            return resultEntityBuilder().status(MessageCode.PROJECT_ID_OR_TABLE_ID_EMPTY).build();
        }
        return service.getEncodeColumns(URLDecoder.decode(rowQueryString, "UTF-8"));
    }

    /**
     {
     "status": 0,
     "message": "ok"payload": {,
     "
        "14": [
            "cdc_target_test_01.T.commitstream"
        ],
        "25": [
            "db2.test.test_rule"
        ]
     }
     }
     * */
    @ApiOperation(value = "get topic list", notes = "获取选中sink 的topic list")
    @GetMapping("/topics")
    @ProjectAuthority
    public ResultEntity getTopicList(@RequestParam Long projectId) {
        return service.getSinkTopics(projectId);
    }

    @ApiOperation(value = "get sink list", notes = "获取sink下拉列表 list")
    @GetMapping("/sinks")
    @ProjectAuthority
    public ResultEntity getSinkList(@RequestParam Integer projectId) {
        if (projectId == null) {
            return resultEntityBuilder().status(MessageCode.PROJECT_ID_EMPTY).build();
        }
        return service.getProjectSinks(projectId);
    }

    @PostMapping(value = "/add", consumes = "application/json")
    public ResultEntity addTable(@RequestBody TableBean tableBean) {
        try {
            ResultEntity result = service.addTable(tableBean);
            if (result == null) {
                resultEntityBuilder().status(MessageCode.TABLE_ADD_LACK_MSG).build();
            }
            return resultEntityBuilder().payload(result).build();
        }catch (Exception e){
            logger.error("[add table]添加table 异常： ",e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 根据projectId和获取table信息，包括：
     * resource: dsName和schemaName等信息
     * sink：sink信息
     * encodes：table下column信息，包括脱敏信息
     */
    @GetMapping("/{projectId}/{projectTableId}")
    @ProjectAuthority
    public ResultEntity queryTableById(@PathVariable Integer projectId, @PathVariable Integer projectTableId) {
        return service.getTableMessage(projectId, projectTableId);
    }

    /**
     * 获取table下column信息,包括源端脱敏信息
     */
    @ApiOperation(value = "source Columns", notes = "table下所有columns，包括源端的脱敏信息")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "tableId", value = "table id.", dataType = "int")
    })
    @GetMapping("/source-columns")
    public ResultEntity getSourceColumns(HttpServletRequest request) throws Exception {
        return service.getColumns(URLDecoder.decode(request.getQueryString(), "UTF-8"));
    }

   /* @GetMapping("/encode-output-columns/{tableId}")
    public ResultEntity getEncodeOutputColumns(@PathVariable Integer tableId) {
        return service.getEncodeOutputColumns(tableId);
    }*/

    @PostMapping(value = "/update", consumes = "application/json")
    public ResultEntity updateTable(@RequestBody TableBean newTable) {
        ResultEntity updateResult = service.updateTable(newTable);
        if(updateResult.getStatus() == ResultEntity.SUCCESS){
            //成功直接返回，
            return updateResult;
        }else {
            // 错误的话，根据Service层的错误码，重新构造返回信息，用来读取message的信息
            return resultEntityBuilder().status(updateResult.getStatus()).build();
        }

    }

    @ApiOperation(value = "getPartitionMsgs", notes = "start table时获取的offset信息")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "topic", value = "input topic", dataType = "string")
    })
    @GetMapping("/partitions")
    public ResultEntity getPartitionMsgs(HttpServletRequest request) throws Exception {
        return service.getTopicOffsets(URLDecoder.decode(request.getQueryString(), "UTF-8"));
    }

    @ApiOperation(value = "getAffectedTables", notes = "search affected tables")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "topic", value = "input topic", dataType = "string")
    })
    @GetMapping("/affected-tables")
    public ResultEntity getAffectedTables(HttpServletRequest request) throws Exception {
        return service.getAffectedTables(URLDecoder.decode(request.getQueryString(), "UTF-8"));
    }

    /**
     * 启动时传入的信息
     * [
     * {tableId:1,topic:'top',topoName:"",partition:'0',offset:'head/latest/1234或者为"" '},
     * {tableId:2,topic:'top',topoName:"",partition:'0',offset:'head/latest/1234或者为"" '}
     * ]
     */
    @PostMapping("/start")
    public ResultEntity startTable(@RequestBody List<ProjectTableOffsetBean> offsets) {
        try {
            service.start(offsets);
            return resultEntityBuilder().build();
        }catch (Exception e){
            return resultEntityBuilder().status(MessageCode.TABLE_PARAM_FORMAT_ERROR).build();
        }

    }

    @GetMapping("/stop")
    public ResultEntity stopTable(@RequestParam("tableId") Integer projectTopoTableId,
                                  @RequestParam("topoName") String topoName) {
        try {
            service.stop(projectTopoTableId,topoName);
            return resultEntityBuilder().build();
        }catch (Exception e){
            return resultEntityBuilder().status(MessageCode.TABLE_NOT_FOUND).build();
        }
    }

    @GetMapping("/reload")
    public ResultEntity reloadTable(@RequestParam("tableId") Integer projectTopoTableId,
                                    @RequestParam("topoName") String topoName) {
        try {
            service.reload(projectTopoTableId,topoName);
            return resultEntityBuilder().build();
        }catch (Exception e){
            return resultEntityBuilder().status(MessageCode.TABLE_NOT_FOUND).build();
        }
    }

    @GetMapping("/delete/{id}")
    public ResultEntity deleteById(@PathVariable("id") Integer id) {
        return service.deleteById(id);
    }

    /**
     * 直接返回，不等待结果
     */
    @GetMapping("/initialLoad")
    public ResultEntity initialLoad(Integer projectTableId, String outputTopic) throws Exception{
        ResultEntity entity = service.fullPull(projectTableId, outputTopic);
        if (entity != null) {//** 非空，说明失败
            //*** 如果非空，说明需要根据i18n自动构造返回想你想
            if(entity.getMessage() == null){
                return resultEntityBuilder().status(entity.getStatus()).build();
            }
            //*** message不为空，说明返回信息完整，直接返回。
            return entity;
        } else {//** 成功直接返回
            return resultEntityBuilder().build();
        }
       /* while(true) {
            if (result.isDone()) {/*//*调用完成
                ResultEntity entity = result.get();
                if (entity != null) {/*//** 非空，说明失败
                    /*//*** 如果非空，说明需要根据i18n自动构造返回想你想
                    if(entity.getMessage() == null){
                        return resultEntityBuilder().status(entity.getStatus()).build();
                    }
                    /*//*** message不为空，说明返回信息完整，直接返回。
                    return entity;
                } else {/*//** 成功直接返回
                    return resultEntityBuilder().build();
                }
            }else {/*//* 调用未完成，等待
                Thread.sleep(100);
            }
        }*/
    }

    @GetMapping("/encoders")
    @ProjectAuthority
    public ResultEntity getAllEncoders(@RequestParam int projectId){
        return service.getAllEncoders(projectId);
    }

}
