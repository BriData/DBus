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
import com.creditease.dbus.bean.AddSchemaAndTablesBean;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.domain.model.DataSchema;
import com.creditease.dbus.service.DataSchemaService;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;

/**
 * User: 尹宏春
 * Date: 2018-05-08
 * Time: 上午11:38
 */
@RestController
@RequestMapping("/data-schema")
@AdminPrivilege
public class DataSchemaController extends BaseController {

    @Autowired
    private DataSchemaService service;

    @ApiOperation(value = "search", notes = "搜索DataSchema")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "dsName", value = "dataSource name", dataType = "String"),
            @ApiImplicitParam(name = "schemaName", value = "schema name", dataType = "String"),
            @ApiImplicitParam(name = "pageNum", value = "page num", dataType = "int"),
            @ApiImplicitParam(name = "pageSize", value = "page size", dataType = "int"),
            @ApiImplicitParam(name = "sortBy", value = "sort by field", dataType = "String"),
            @ApiImplicitParam(name = "order", value = "order: asc/desc", dataType = "String")
    })
    @GetMapping("/search")
    public ResultEntity search(HttpServletRequest request) throws Exception {
        return service.searchSchemaAndDs(request.getQueryString());
    }

    /**
     * Param:
     * schemaId      required=false
     * dsId          required=false
     * schemaName    required=false
     */
    @GetMapping("/searchAll")
    public ResultEntity searchAll(HttpServletRequest request) throws Exception {
        return service.searchSchema(request.getQueryString());
    }

    /**
     * Param:
     * schemaId      required=false
     * dsId          required=false
     * schemaName    required=false
     */
    @GetMapping("/findByIdOrName")
    public ResultEntity findByIdOrName(HttpServletRequest request) {
        return service.searchSchema(request.getQueryString());
    }

    /**
     * Param:
     * schemaId      required=false
     * dsId          required=false
     * schemaName    required=false
     */
    @GetMapping("/checkschema")
    public ResultEntity checkschema(HttpServletRequest request) {
        return service.searchSchema(request.getQueryString());
    }

    /**
     * Param:
     * schemaId      required=false
     * dsId          required=false
     * schemaName    required=false
     * 根据 ID获取dataSchema列表
     *
     * @return dataSchema列表
     */
    @GetMapping("/findByDsId")
    public ResultEntity findByDsId(HttpServletRequest request) {
        return service.searchSchema(request.getQueryString());
    }

    /**
     * 使指定的schema生效或失效
     *
     * @param id 数据源ID
     */
    @PostMapping("{id:[0-9]+}/{status}")
    public ResultEntity changeStatus(@PathVariable Long id, @PathVariable String status) {
        return service.modifyDataSchemaStatus(id, status);
    }

    @PostMapping("/insert")
    public ResultEntity addOne(@RequestBody DataSchema newOne) {
        return service.insertOne(newOne);
    }

    @GetMapping("/delete/{id}")
    public ResultEntity deleteById(@PathVariable Integer id) {
        try {
            int size = service.countActiveTables(id);
            if (size != 0) {
                return resultEntityBuilder().status(MessageCode.SCHEMA_IS_USING).build();
            }
            ResultEntity resultEntity = service.delete(id);
            if (resultEntity.getMessage() != null) {
                return resultEntity;
            }
            return resultEntityBuilder().status(resultEntity.getStatus()).build();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping("/update")
    public ResultEntity updateById(@RequestBody DataSchema updateOne) throws Exception {
        return service.update(updateOne);
    }


    /**
     * 根据dataSource name获取dataSchema列表
     *
     * @return dataSchema列表
     */
    @GetMapping("/schemaname")
    public ResultEntity fetchSchema(HttpServletRequest request) {
        return service.fetchSchemaFromSource(request.getQueryString());
    }

    /**
     * 根据DataSourceId,获取源端schema信息
     */
    @GetMapping("/source-schemas")
    public ResultEntity getSourceSchemas(HttpServletRequest request) {
        return service.fetchSchemaFromSourceByDsId(request.getQueryString());
    }

    /**
     * 获取managerScheme信息 以及 可添加的table列表
     */
    @GetMapping("/schema-tables")
    public ResultEntity getSchemaAndTables(@RequestParam int dsId, @RequestParam String dsName,
                                           @RequestParam String schemaName) {
        return service.getSchemaAndTablesInfo(dsId, dsName, schemaName);
    }

    @PostMapping("/schema-tables")
    public ResultEntity addSchemaAndTables(@RequestBody AddSchemaAndTablesBean schemaAndTablesInfo) {
        try {
            ResultEntity resultEntity = service.addSchemaAndTablesInfo(schemaAndTablesInfo);
            if (resultEntity.getMessage() == null) {
                return resultEntityBuilder().status(resultEntity.getStatus()).build();
            }
            return resultEntity;

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * schema 级别拖回重跑,对整个schema均有影响
     *
     * @param dsId
     * @param dsName
     * @param schemaName
     * @param offset
     * @return
     */
    @GetMapping("/rerun")
    public ResultEntity rerun(Integer dsId, String dsName, String schemaName, Long offset) {
        try {
            int result = service.rerun(dsId, dsName, schemaName, offset);
            return resultEntityBuilder().status(result).build();
        } catch (Exception e) {
            logger.error("Exception encountered while rerun schema ({})", dsName + "." + schemaName, e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

}
