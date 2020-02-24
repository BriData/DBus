package com.creditease.dbus.controller;

import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.domain.model.SinkerTopologySchema;
import com.creditease.dbus.service.SinkerSchemaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/sinkerSchema")
public class SinkerSchemaController extends BaseController {

    @Autowired
    private SinkerSchemaService service;

    @GetMapping(path = "/getById/{id}")
    public ResultEntity getById(@PathVariable Integer id) {
        try {
            return resultEntityBuilder().payload(service.getById(id)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while get sinker topology schema by id", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping(path = "/search")
    public ResultEntity search(Integer pageNum, Integer pageSize, String dsName, String schemaName, String sinkerName, String targetTopic) {
        pageNum = pageNum == null ? 1 : pageNum;
        pageSize = pageSize == null ? 10 : pageSize;
        try {
            return resultEntityBuilder().payload(service.search(pageNum, pageSize, dsName, schemaName, sinkerName, targetTopic)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while get sinker topology schema", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping(path = "/searchNotPage")
    public ResultEntity searchNotPage(String dsName, String schemaName, String sinkerName, String targetTopic) {
        try {
            return resultEntityBuilder().payload(service.searchNotPage(dsName, schemaName, sinkerName, targetTopic)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while get sinker topology schema", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping(path = "/update")
    public ResultEntity update(@RequestBody SinkerTopologySchema schema) {
        try {
            service.update(schema);
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while update sinker topology schema", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping(path = "/delete/{id}")
    public ResultEntity delete(@PathVariable Integer id) {
        try {
            return resultEntityBuilder().payload(service.delete(id)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while update sinker topology schema", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * left join t_data_schema表的数据
     */
    @GetMapping(path = "/searchAll")
    public ResultEntity searchAll(String dsName, String schemaName, Integer sinkerTopoId) {
        try {
            return resultEntityBuilder().payload(service.searchAll(dsName, schemaName, sinkerTopoId)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while get sinker topology schema", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping(path = "/addSchemas")
    public ResultEntity addSchemas(@RequestBody List<SinkerTopologySchema> sinkerSchemaList) {
        try {
            return resultEntityBuilder().payload(service.addSchemas(sinkerSchemaList)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while add sinker topology schemas", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 仅包含t_sinker_topo_schema表数据
     */
    @GetMapping(path = "/selectAll")
    public ResultEntity selectAll() {
        try {
            return resultEntityBuilder().payload(service.selectAll()).build();
        } catch (Exception e) {
            logger.error("Exception encountered while get sinker topology schema", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

}
