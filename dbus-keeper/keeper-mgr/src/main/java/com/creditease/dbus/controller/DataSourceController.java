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
import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.domain.model.DataSource;
import com.creditease.dbus.service.DataSourceService;
import com.creditease.dbus.utils.StormToplogyOpHelper;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.Map;

/**
 * User: 王少楠
 * Date: 2018-05-08
 * Time: 上午11:38
 */
@RestController
@RequestMapping("/data-source")
@AdminPrivilege
public class DataSourceController extends BaseController {

    @Autowired
    private DataSourceService service;
    @Autowired
    private IZkService zkService;

    @ApiOperation(value = "search", notes = "搜索DataSource")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "dsName", value = "dataSource name", dataType = "String"),
            @ApiImplicitParam(name = "pageNum", value = "page num", dataType = "int"),
            @ApiImplicitParam(name = "pageSize", value = "page size", dataType = "int"),
            @ApiImplicitParam(name = "sortBy", value = "sort by field", dataType = "String"),
            @ApiImplicitParam(name = "order", value = "order: asc/desc", dataType = "String")
    })
    @GetMapping("/search")
    public ResultEntity search(HttpServletRequest request) throws Exception {
        return service.search(request.getQueryString());
    }

    @GetMapping("/{id}")
    public ResultEntity getById(@PathVariable Integer id) {
        return service.getById(id);
    }

    @PostMapping("")
    public ResultEntity addOne(@RequestBody DataSource newOne) {
        try {
            //插入数据库
            ResultEntity resultEntity = service.insertOne(newOne);
            if (resultEntity.getStatus() != 0) {
                return resultEntity;
            }
            //自动部署ogg或者canal
            Integer dsId = resultEntity.getPayload(Integer.class);
            return resultEntityBuilder().status(service.autoAddOggCanalLine(newOne)).payload(dsId).build();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping("/delete/{id}")
    public ResultEntity deleteById(@PathVariable Integer id) {
        try {
            int size = service.countActiveTables(id);
            if (size != 0) {
                return resultEntityBuilder().status(MessageCode.DATASOURCE_ALREADY_BE_USING).build();
            }
            ResultEntity resultEntity = service.delete(id);
            if (resultEntity.getMessage() == null) {
                return resultEntityBuilder().status(resultEntity.getStatus()).build();
            }
            return resultEntity;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping("/update")
    public ResultEntity updateById(@RequestBody DataSource updateOne) throws Exception {
        return service.update(updateOne);
    }

    /**
     * 根据数据源名称获取数据源列表。
     *
     * @param name 如果name不存在则返回全部数据源
     * @return 数据源列表
     * <p>
     * 旧接口:findDataSources(@QueryParam("name") String name)
     * 旧接口：另一个接口 @GetMapping("/checkName")调用逻辑与此相同。合并了。请调用此处代码。
     */
    @GetMapping("/getDataSourceByName")
    public ResultEntity getDataSourceByName(@RequestParam(required = false) String name) {
        return service.getDataSourceByName(name);
    }

    /**
     * 源端数据访问
     */
    @GetMapping("/searchFromSource")
    public ResultEntity searchFromSource(@RequestParam Integer dsId) {
        return service.searchFromSource(dsId);
    }

    /**
     * 检验数据源的能否连通
     *
     * @param map {"dsType":"","masterURL":"","slaverURL":"","user":"","password":""}
     * @return
     */
    @PostMapping("/validate")
    public ResultEntity validateDataSources(@RequestBody Map<String, Object> map) {
        return service.validateDataSources(map);
    }

    /**
     * 使指定的数据源生效或失效
     *
     * @param id 数据源ID
     */
    @PostMapping("{id:[0-9]+}/{status}")
    public ResultEntity changeStatus(@PathVariable Long id, @PathVariable String status) {
        return service.modifyDataSourceStatus(id, status);
    }

    @GetMapping("/getDSNames")
    public ResultEntity getDSNames() {
        return service.getDSNames();
    }

    @PostMapping("/kill/{topologyId}/{waitTime}")
    public ResultEntity killTopology(@PathVariable String topologyId, @PathVariable Integer waitTime) {
        if (StringUtils.isBlank(topologyId)) {
            return resultEntityBuilder().status(MessageCode.DATASOURCE_KILL_TOPO_NONE_TOPO_ID).build();
        }

        if (waitTime == null) {
            waitTime = 10;
        }

        try {
            if (!StormToplogyOpHelper.inited) {
                StormToplogyOpHelper.init(zkService);
            }
            String killResult = StormToplogyOpHelper.killTopology(topologyId, waitTime);
            if (StringUtils.isNotBlank(killResult) && killResult.equals(StormToplogyOpHelper.OP_RESULT_SUCCESS)) {
                return new ResultEntity(ResultEntity.SUCCESS, ResultEntity.OK);
            } else {
                return new ResultEntity(MessageCode.DATASOURCE_KILL_TOPO_FAILED, ResultEntity.FAILED);
            }
        } catch (Exception e) {
            logger.warn("Kill Storm Topology Failed!", e);
            return resultEntityBuilder().status(MessageCode.DATASOURCE_KILL_TOPO_EXCEPTION).build();
        }
    }

    @PostMapping(path = "startTopology", consumes = "application/json")
    public ResultEntity startTopology(@RequestBody Map<String, String> map) {
        try {
            String dsName = map.get("dsName");
            String jarPath = map.get("jarPath");
            String jarName = map.get("jarName");
            String topologyType = map.get("topologyType");
            String opResult = service.startTopology(dsName, jarPath, jarName, topologyType);
            return resultEntityBuilder().payload(opResult).build();
        } catch (Exception e) {
            logger.warn("Start Storm Topology Failed!", e);
            return resultEntityBuilder().status(MessageCode.DATASOURCE_START_TOPO_FAILED).build();
        }
    }

    @ApiOperation(value = "getPath", notes = "获取启动topology时的jar path信息")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "dsId", value = "dataSource id", dataType = "Integer")
    })
    @GetMapping("/paths")
    public ResultEntity getPath(HttpServletRequest request) {
        return service.getPath(request.getQueryString());
    }

    @GetMapping("/view-log")
    public ResultEntity viewLog(String topologyId) throws Exception {
        return resultEntityBuilder().payload(service.viewLog(topologyId)).build();
    }

    /**
     * 数据线级别别拖回重跑,对整条数据线均有影响
     *
     * @param dsId
     * @param dsName
     * @param offset
     * @return
     */
    @GetMapping("/rerun")
    public ResultEntity rerun(Integer dsId, String dsName, Long offset) {
        try {
            int result = service.rerun(dsId, dsName, offset);
            return resultEntityBuilder().status(result).build();
        } catch (Exception e) {
            logger.error("Exception encountered while rerun datasource ({})", dsName, e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }
}
