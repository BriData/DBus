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
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.service.ToolSetService;
import com.creditease.dbus.service.TopologyService;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Map;


/**
 * Created by xiancangao on 2018/05/04.
 */
@RestController
@RequestMapping("/toolSet")
@AdminPrivilege
public class ToolSetController extends BaseController {
    @Autowired
    private ToolSetService toolSetService;
    @Autowired
    private TopologyService topoLogyService;

    /**
     * 发送controMessage
     * 参数 type(message类型),message,topic
     *
     * @param map
     * @return
     */
    @PostMapping(path = "sendCtrlMessage", consumes = "application/json")
    public ResultEntity sendCtrlMessage(@RequestBody Map<String, String> map) {
        try {
            return resultEntityBuilder().status(toolSetService.sendCtrlMessage(map)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while request sendCtrlMessage", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }


    @GetMapping(path = "batchSendControMessage")
    public ResultEntity batchSendControMessage(@RequestParam String reloadType) {
        try {
            return resultEntityBuilder().status(toolSetService.batchSendControMessage(reloadType)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while batchSendControMessage", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping("/readZKNode")
    public ResultEntity readZKNode(@RequestParam String type) {
        try {
            return resultEntityBuilder().payload(toolSetService.readZKNode(type)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while request readZKNode.", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping(path = "globalFullPull", consumes = "application/json")
    public ResultEntity globalFullPull(@RequestBody Map<String, String> map) {
        try {
            Integer result = toolSetService.globalFullPull(map);
            if (result != null) {
                return resultEntityBuilder().status(result).build();
            }
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while request globalFullPull", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping("/getGlobalFullPullTopo")
    public ResultEntity getGlobalFullPullTopo() {
        try {
            if (StringUtils.isBlank(toolSetService.getGlobalFullPullTopo())) {
                return resultEntityBuilder().payload(false).build();
            }
            return resultEntityBuilder().payload(true).build();
        } catch (Exception e) {
            logger.error("Exception encountered while request getGlobalFullPullTopo", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping("/killGlobalFullPullTopo")
    public ResultEntity killGlobalFullPullTopo() {
        try {
            return resultEntityBuilder().status(toolSetService.killGlobalFullPullTopo()).build();
        } catch (Exception e) {
            logger.error("Exception encountered while request getGlobalFullPullTopo", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 读取kafka消息工具类
     * map{
     * topic:查询的具体topic,
     * from:起始offset,
     * length:from之后查询多少个offset,
     * params:过滤参数,多个参数使用","隔开
     * }
     *
     * @param map
     * @return
     */
    @PostMapping(path = "kafkaReader", consumes = "application/json")
    public ResultEntity kafkaReader(@RequestBody Map<String, String> map) {
        try {
            return toolSetService.kafkaReader(map, currentUserId(), currentUserRole());
        } catch (Exception e) {
            logger.error("Exception encountered while request kafkaReader", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * @param bootstrapServers 暂时不需要,预留
     * @param param            topic名称过滤参数
     * @return
     */
    @GetMapping("getTopics")
    public ResultEntity getTopics(String bootstrapServers, String param) {
        try {
            return resultEntityBuilder().payload(toolSetService.getTopics(bootstrapServers, param)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while request getTopics", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * @param bootstrapServers 暂时不需要,预留
     * @param param            topic名称过滤参数
     * @return
     */
    @GetMapping("getTopicsByUser")
    public ResultEntity getTopicsByUserId(String bootstrapServers, String param) {
        try {
            return resultEntityBuilder().payload(toolSetService.getTopicsByUserId(bootstrapServers, param, currentUserId(), currentUserRole())).build();
        } catch (Exception e) {
            logger.error("Exception encountered while request getTopics", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 获取对应topic的 begin和end  offset
     *
     * @param bootstrapServers 暂时不需要,预留
     * @param topic
     * @return
     */
    @GetMapping("getOffset")
    public ResultEntity getOffset(String bootstrapServers, String topic) {
        try {
            return resultEntityBuilder().payload(toolSetService.getOffset(bootstrapServers, topic)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while request getOffset", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping("/sendCtrlMessageEasy")
    public ResultEntity sendCtrlMessageEasy(Integer dsId, String dsName, String dsType) {
        try {
            toolSetService.sendCtrlMessageEasy(dsId, dsName, dsType);
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while request sendCtrlMessageEasy", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping("/sourceTableColumn/{tableId}/{number}")
    public ResultEntity sourceTableColumn(@PathVariable Integer tableId, @PathVariable Integer number) {
        if (number < 10 || number > 50) {
            return resultEntityBuilder().status(MessageCode.LINE_NUMBER_IS_WRONG).build();
        }
        try {
            return toolSetService.sourceTableColumn(tableId, number);
        } catch (Exception e) {
            logger.error("Exception encountered while request SourceTableColumn", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping("/checkEnvironment")
    public ResultEntity checkEnvironment() {
        try {
            return resultEntityBuilder().payload(toolSetService.checkEnvironment()).build();
        } catch (Exception e) {
            logger.error("Exception encountered while request checkEnvironment", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping("/initConfig")
    public ResultEntity initConfig(String dsName) {
        try {
            toolSetService.initConfig(dsName);
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while request initConfig", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping("/batchRestartTopo")
    public ResultEntity batchRestartTopo(@RequestBody Map<String,Object> map) {
        try {
            String dsType = (String) map.get("dsType");
            String jarPath = (String) map.get("jarPath");
            String topoType = (String) map.get("topoType");
            ArrayList<String> dsNameList = (ArrayList<String>) map.get("dsNameList");
            logger.info("************* batch restart topo start dsType:{},topoType:{},jarPath:{},dsNameList:{} " +
                    "************* ", dsType, topoType, jarPath, dsNameList);
            if ("mysql".equalsIgnoreCase(dsType)) {
                if (topoType.equalsIgnoreCase("log-processor")) {
                    return resultEntityBuilder().status(MessageCode.TOPO_TYPE_AND_DSTYPE_NOT_MATCH).build();
                }
            } else if ("oracle".equalsIgnoreCase(dsType)) {
                if (topoType.equalsIgnoreCase("log-processor")
                        || topoType.equalsIgnoreCase("mysql-extractor")) {
                    return resultEntityBuilder().status(MessageCode.TOPO_TYPE_AND_DSTYPE_NOT_MATCH).build();
                }
            }
            else {
                if (topoType.equalsIgnoreCase("splitter-puller")
                        || topoType.equalsIgnoreCase("dispatcher-appender")
                        || topoType.equalsIgnoreCase("mysql-extractor")
                        || topoType.equalsIgnoreCase("dispatcher")
                        || topoType.equalsIgnoreCase("appender")) {
                    return resultEntityBuilder().status(MessageCode.TOPO_TYPE_AND_DSTYPE_NOT_MATCH).build();
                }
            }
            topoLogyService.batchRestartTopo(jarPath, topoType, dsNameList);
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while request batchRestartTopo", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

}
