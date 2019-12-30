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

import com.creditease.dbus.annotation.AdminPrivilege;
import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.service.ConfigCenterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;


/**
 * Created by xiancangao on 2018/05/31
 */
@RestController
@RequestMapping("/configCenter")
@AdminPrivilege
public class ConfigCenterController extends BaseController {
    @Autowired
    private ConfigCenterService configCenterService;

    /**
     * 修改 globalConf 配置
     */
    @PostMapping(path = "updateGlobalConf", consumes = "application/json")
    public ResultEntity updateGlobalConf(@RequestBody LinkedHashMap<String, String> map) {
        try {
            return configCenterService.updateGlobalConf(map);
        } catch (Exception e) {
            logger.error("Exception encountered while send updateGlobalConf with param ( map:{})}", map, e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 修改 MgrDB 配置
     */
    @PostMapping(path = "updateMgrDB", consumes = "application/json")
    public ResultEntity updateMgrDB(@RequestBody Map<String, String> map) {
        try {
            return configCenterService.updateMgrDB(map);
        } catch (Exception e) {
            logger.error("Exception encountered while send updateMgrDB with param ( map:{})}", map, e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 获取基础配置,包括zk,kafka,mgrDB地址
     */
    @GetMapping("/getBasicConf")
    public ResultEntity getBasicConf() {
        try {
            return configCenterService.getBasicConf();
        } catch (Exception e) {
            logger.error("Exception encountered while getBasicConf ", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 该功能不建议使用,太危险!!!!
     * 重置mgr数据库表结构
     */
    //@PostMapping(path = "/ResetMgrDB", consumes = "application/json")
    //public ResultEntity ResetMgrDB(@RequestBody LinkedHashMap<String, String> map) {
    //    try {
    //        int i = configCenterService.ResetMgrDB(map);
    //        return resultEntityBuilder().status(i).build();
    //    } catch (Exception e) {
    //        logger.error("Exception encountered while ResetMgrDB ", e);
    //        return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
    //    }
    //}


    /**
     * 环境是否已经初始化过,根据zookeeper有无/DBus节点判断
     *
     * @return
     */
    @GetMapping(path = "/isInitialized")
    public ResultEntity isInitialized() {
        try {
            return resultEntityBuilder().payload(configCenterService.isInitialized()).build();
        } catch (Exception e) {
            logger.error("Exception encountered while isInitialized ", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping(path = "/sendMailTest")
    public ResultEntity sendMailTest(@RequestBody Map<String, Object> map) {
        try {
            return resultEntityBuilder().payload(configCenterService.sendMailTest(map)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while sendMailTest ", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

}
