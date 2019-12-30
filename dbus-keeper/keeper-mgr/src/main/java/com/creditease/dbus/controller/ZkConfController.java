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
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.domain.model.ZkNode;
import com.creditease.dbus.service.ConfigCenterService;
import com.creditease.dbus.service.ZkConfService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by xiancangao on 2018/05/04.
 */
@RestController
@RequestMapping("/zookeeper")
@AdminPrivilege
public class ZkConfController extends BaseController {

    @Autowired
    private ZkConfService zkConfService;
    @Autowired
    private ConfigCenterService configCenterService;

    /**
     * 按照properties格式返回节点信息
     *
     * @param path
     * @return
     */
    @GetMapping("/loadZKNodeProperties")
    public ResultEntity loadZKNodeProperties(@RequestParam String path) {
        try {
            Properties properties = zkConfService.loadZKNodeProperties(path);
            //没有
            if ((properties == null || properties.isEmpty()) && path.endsWith(Constants.GLOBAL_PROPERTIES)) {
                Map map = configCenterService.getBasicConf().getPayload(Map.class);
                if (map != null && !map.isEmpty()) {
                    properties = new Properties();
                    properties.putAll(map);
                }
            }
            return resultEntityBuilder().payload(properties).build();
        } catch (Exception e) {
            logger.error("Exception encountered while request loadZKNodeInfo with param ( path:{}).", path, e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 按照字符串格式返回节点信息
     *
     * @param path
     * @return
     */
    @GetMapping("/loadZKNodeJson")
    public ResultEntity loadZKNodeJson(@RequestParam String path, String admin) {
        try {
            ZkNode zkNode = zkConfService.loadZKNodeJson(path, admin);
            return resultEntityBuilder().payload(zkNode).build();
        } catch (Exception e) {
            logger.error("Exception encountered while request loadZKNodeJson with param ( path:{}).", path, e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 按照properties保存更新节点信息
     *
     * @param map
     * @param path
     * @return
     */
    @PostMapping(path = "/updateZKNodeProperties", consumes = "application/json")
    public ResultEntity updateZKNodeProperties(@RequestBody Map<String, Object> map, @RequestParam String path) {
        try {
            zkConfService.updateZKNodeProperties(path, map);
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while request updateZKNodeProperties with param ( map:{},path:{}).", map, path, e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 按照存文本保存更新节点信息(传入Json字符串,节点信息放入content节点)
     *
     * @param map
     * @param path
     * @return
     */
    @PostMapping(path = "/updateZKNodeJson", consumes = "application/json")
    public ResultEntity updateZKNodeJson(@RequestBody Map<String, String> map, @RequestParam String path) {
        try {
            zkConfService.updateZKNodeJson(path, map.get("content"));
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while request updateZKNodeJson with param ( map:{},path:{}).", map, path, e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 根据path获取对应zk树。
     *
     * @return zk tree json
     */
    @GetMapping("/loadZkTreeOfPath")
    public ResultEntity loadZkTreeOfPath(@RequestParam String path) {

        try {
            ZkNode root = zkConfService.loadZkTreeOfPath(path);
            return resultEntityBuilder().payload(root).build();
        } catch (Exception e) {
            logger.error("Exception encountered while request loadZkTreeOfPath with param ( path:{}).", path, e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 根据path获取对应节点的zk子树。
     *
     * @return zk tree json
     */
    @GetMapping("/loadSubTreeOfPath")
    public ResultEntity loadSubTreeOfPath(@RequestParam String path) {
        try {
            List<ZkNode> children = zkConfService.loadSubTreeOfPath(path);
            return resultEntityBuilder().payload(children).build();
        } catch (Exception e) {
            logger.error("Exception encountered while request loadSubTreeOfPath with param ( path:{}).", path, e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 根据path获取对应节点的zk子树。
     * 每次获取三层目录
     *
     * @return zk tree json
     */
    @GetMapping("/loadLevelOfPath")
    public ResultEntity loadLevelOfPath(@RequestParam String path) {
        try {
            ZkNode nodeOfPath = zkConfService.loadLevelOfPath(path);
            return resultEntityBuilder().payload(nodeOfPath).build();
        } catch (Exception e) {
            logger.error("Exception encountered while request loadLevelOfPath with param ( path:{}).", path, e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 根据给定dsName获取对应业务线的zk配置树。
     * 特别地：我们获取business tree的时候,会考虑“以zk配置模板为基准”。
     * 我们获取完整的zk配置模板树,然后根据该树的结构和映射规则,去查询业务的对应zk配置。
     * 如果没找到,会设置exists=false标志。
     * 所以,返回给前端的数据,实际上是zk模板树,但其中的path等根据规则替换成了对应business的path。
     * 并根据业务node是否存在,设置标志。
     *
     * @return zk tree json
     */
    @GetMapping("/loadZkTreeByDsName")
    public ResultEntity loadZkTreeByDsName(@RequestParam String dsName, @RequestParam String dsType) {
        try {
            ZkNode root = zkConfService.loadZkTreeByDsName(dsName, dsType);
            return resultEntityBuilder().payload(root).build();
        } catch (Exception e) {
            logger.error("Exception encountered while request loadZkTreeByDsName with param ( dsName:{},dsType:{}).", dsName, dsType, e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 根据path,添加新节点到对应zk树。
     *
     * @return
     */
    @GetMapping(path = "/addZkNodeOfPath")
    public ResultEntity addZkNodeOfPath(@RequestParam("path") String path, @RequestParam("nodeName") String nodeName) {
        try {
            zkConfService.addZkNodeOfPath(path, nodeName);
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while request addZkNodeOfPath with param ( path:{},nodeName:{}).", path, nodeName, e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 根据path,删除对应zk节点。
     *
     * @return
     */
    @GetMapping("/deleteZkNodeOfPath")
    public ResultEntity deleteZkNodeOfPath(@RequestParam String path) {
        try {
            String s = zkConfService.deleteZkNodeOfPath(path);
            if (s != null) {
                return new ResultEntity(MessageCode.EXCEPTION, s);
            }
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while request deleteZkNodeOfPath with param ( path:{}).", path, e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * * clone configure from template
     */
    @GetMapping("/cloneConfFromTemplate")
    public ResultEntity cloneConfFromTemplate(@RequestParam String dsName, @RequestParam String dsType) {
        try {
            zkConfService.cloneConfFromTemplate(dsName, dsType);
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while request cloneConfFromTemplate with param ( dsName:{},dsType:{}).", dsName, dsType, e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }
}
