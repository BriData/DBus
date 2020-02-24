package com.creditease.dbus.controller;

import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.domain.model.SinkerTopologyTable;
import com.creditease.dbus.service.SinkerTableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/sinkerTable")
public class SinkerTableController extends BaseController {

    @Autowired
    private SinkerTableService service;

    @GetMapping(path = "/search")
    public ResultEntity search(Integer pageNum, Integer pageSize, String dsName, String schemaName, String tableName, String sinkerName) {
        pageNum = pageNum == null ? 1 : pageNum;
        pageSize = pageSize == null ? 10 : pageSize;
        try {
            return resultEntityBuilder().payload(service.search(pageNum, pageSize, dsName, schemaName, tableName, sinkerName)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while get sinker topology table", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping(path = "/searchAll")
    public ResultEntity searchAll(Integer schemaId) {
        try {
            return resultEntityBuilder().payload(service.searchAll(schemaId)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while get sinker topology table", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping("/addSinkerTables")
    public ResultEntity addSinkerTables(@RequestBody List<SinkerTopologyTable> addTables) {
        try {
            return resultEntityBuilder().payload(service.addSinkerTables(addTables)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while get sinker topology table", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping(path = "/delete/{id}")
    public ResultEntity delete(@PathVariable Integer id) {
        try {
            return resultEntityBuilder().payload(service.delete(id)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while update sinker topology table", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping(path = "/update")
    public ResultEntity update(@RequestBody SinkerTopologyTable table) {
        try {
            service.update(table);
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while update sinker topology table", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

}
