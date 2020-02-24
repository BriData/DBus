package com.creditease.dbus.controller;

import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.domain.model.SinkerTopologyTable;
import com.creditease.dbus.service.SinkerTableService;
import org.apache.catalina.servlet4preview.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.net.URLDecoder;

@RestController
@RequestMapping("/sinkerTable")
public class SinkerTableController extends BaseController {

    @Autowired
    private SinkerTableService service;

    @GetMapping(path = "/search")
    public ResultEntity search(HttpServletRequest request) {
        try {
            return service.search(URLDecoder.decode(request.getQueryString(), "UTF-8"));
        } catch (Exception e) {
            logger.error("Exception encountered while get sinker topology table", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping(path = "/searchAll")
    public ResultEntity searchSinkerTopologySchema(HttpServletRequest request) {
        try {
            return service.searchAll(URLDecoder.decode(request.getQueryString(), "UTF-8"));
        } catch (Exception e) {
            logger.error("Exception encountered while get sinker topology table", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping(path = "/delete/{id}")
    public ResultEntity delete(@PathVariable Integer id) {
        try {
            service.delete(id);
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while delete sinker topology table", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping(path = "/update")
    public ResultEntity update(@RequestBody SinkerTopologyTable table) {
        try {
            return service.update(table);
        } catch (Exception e) {
            logger.error("Exception encountered while update sinker topology table", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

}
