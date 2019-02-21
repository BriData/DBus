package com.creditease.dbus.controller;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.annotation.AdminPrivilege;
import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.domain.model.DataSource;
import com.creditease.dbus.service.AutoDeployDataLineService;
import com.creditease.dbus.service.DataSourceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * This is Description
 *
 * @author xiancangao
 * @date 2018/12/11
 */
@RestController
@RequestMapping("/autoDeploy")
@AdminPrivilege
public class AutoDeployDataLineController extends BaseController {
    @Autowired
    private AutoDeployDataLineService service;
    @Autowired
    private DataSourceService dataSourceService;

    @RequestMapping(path = "getOggConf")
    public ResultEntity getOggConf(String dsName) {
        try {
            JSONObject result = service.getOggConf(dsName);
            return resultEntityBuilder().payload(result).build();
        } catch (Exception e) {
            logger.error("Exception encountered while getOggConf with param ( dsName:{})}", dsName, e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping(path = "setOggConf", consumes = "application/json")
    public ResultEntity setOggConf(@RequestBody Map<String, String> map) {
        try {
            service.setOggConf(map);
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while getOggConf with param ( map:{})}", map, e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @RequestMapping(path = "getCanalConf")
    public ResultEntity getCanalConf(String dsName) {
        try {
            JSONObject result = service.getCanalConf(dsName);
            return resultEntityBuilder().payload(result).build();
        } catch (Exception e) {
            logger.error("Exception encountered while getCanalConf with param ( dsName:{})}", dsName, e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping(path = "setCanalConf", consumes = "application/json")
    public ResultEntity setCanalConf(@RequestBody Map<String, String> map) {
        try {
            service.setCanalConf(map);
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while setCanalConf with param ( map:{})}", map, e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @RequestMapping(path = "autoAddLine")
    public ResultEntity autoAddLine(String dsName, String canalUser, String canalPass) {
        try {
            DataSource dataSource = dataSourceService.getDataSourceByDsName(dsName);
            if (dataSource != null) {
                String dsType = dataSource.getDsType();
                if (dsType.equalsIgnoreCase("mysql")) {
                    return resultEntityBuilder().status(service.addCanalLine(dsName, canalUser, canalPass)).build();
                }
                if (dsType.equalsIgnoreCase("oracle")) {
                    return resultEntityBuilder().status(service.addOracleLine(dsName)).build();
                }
            }
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while autoAddLine with param ( dsName:{})}", dsName, e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @RequestMapping(path = "getOggTrailName")
    public ResultEntity getOggTrailName() {
        try {
            return resultEntityBuilder().payload(service.getOggTrailName()).build();
        } catch (Exception e) {
            logger.error("Exception encountered while getOggTrailName ", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

}
