package com.creditease.dbus.controller;

import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.domain.model.SinkerTopologySchema;
import com.creditease.dbus.service.SinkerSchemaService;
import org.apache.catalina.servlet4preview.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.net.URLDecoder;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/sinkerSchema")
public class SinkerSchemaController extends BaseController {

    @Autowired
    private SinkerSchemaService service;

    @GetMapping(path = "/search")
    public ResultEntity search(HttpServletRequest request) {
        try {
            return service.search(URLDecoder.decode(request.getQueryString(), "UTF-8"));
        } catch (Exception e) {
            logger.error("Exception encountered while get sinker topology schema", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping(path = "/update")
    public ResultEntity update(@RequestBody SinkerTopologySchema schema) {
        try {
            return service.update(schema);
        } catch (Exception e) {
            logger.error("Exception encountered while update sinker topology schema", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping(path = "/delete/{id}")
    public ResultEntity delete(@PathVariable Integer id) {
        try {
            service.delete(id);
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while delete sinker topology schema", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping(path = "/searchAll")
    public ResultEntity searchSinkerTopologySchema(HttpServletRequest request) {
        try {
            return service.searchAll(URLDecoder.decode(request.getQueryString(), "UTF-8"));
        } catch (Exception e) {
            logger.error("Exception encountered while get sinker topology schema", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping(path = "/addSinkerTables")
    public ResultEntity addSinkerTables(@RequestBody Map<String, Object> param) {
        try {
            service.addSinkerTables(param);
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while add sinker topology tables", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping(path = "/batchAddSinkerTables")
    public ResultEntity batchAddSinkerTables(@RequestBody List<SinkerTopologySchema> schemas) {
        try {
            service.batchAddSinkerTables(schemas);
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while batch add sinker topology tables", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping(path = "/batchDeleteSinkerSchema")
    public ResultEntity batchDeleteSinkerSchema(@RequestBody List<Integer> ids) {
        try {
            service.batchDeleteSinkerSchema(ids);
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while batch delete sinker topology schema", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

}
