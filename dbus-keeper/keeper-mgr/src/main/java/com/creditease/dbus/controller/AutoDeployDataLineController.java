/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2019 Bridata
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

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.annotation.AdminPrivilege;
import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.bean.DeployInfoBean;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.domain.model.DataSource;
import com.creditease.dbus.service.AutoDeployDataLineService;
import com.creditease.dbus.service.DataSourceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

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

    @GetMapping(path = "getOggConf")
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

    @GetMapping(path = "getCanalConf")
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

    @GetMapping(path = "autoAddLine")
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

    @GetMapping(path = "getOggTrailName")
    public ResultEntity getOggTrailName(Integer number) {
        try {
            return resultEntityBuilder().payload(service.getOggTrailName(number)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while getOggTrailName ", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 机器部署配置获取
     *
     * @return
     */
    @GetMapping(path = "getOggCanalDeployInfo")
    public ResultEntity getOggCanalDeployInfo() {
        try {
            return resultEntityBuilder().payload(service.getOggCanalDeployInfo()).build();
        } catch (Exception e) {
            logger.error("Exception encountered while getOggCanalDeployInfo ", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 机器部署配置更新
     *
     * @return
     */
    @PostMapping(path = "updateOggCanalDeployInfo")
    public ResultEntity updateOggCanalDeployInfo(@RequestBody DeployInfoBean deployInfoBean) {
        try {
            service.updateOggCanalDeployInfo(deployInfoBean);
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while updateOggCanalDeployInfo ", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 更新canal,ogg的部署详情到zk节点
     *
     * @return
     */
    @GetMapping(path = "syncOggCanalDeployInfo")
    public ResultEntity syncOggCanalDeployInfo() {
        try {
            return resultEntityBuilder().payload(service.syncOggCanalDeployInfo()).build();
        } catch (Exception e) {
            logger.error("Exception encountered while syncOggCanalDeployInfo ", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

}
