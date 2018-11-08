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

package com.creditease.dbus.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.common.utils.JsonUtil;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.ZkService;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 用于topology的停止
 * @author 201603030496
 *
 */
public class CommandCtrl {
    private static final Logger LOG = LoggerFactory.getLogger(CommandCtrl.class);

    public static String killTopology(ZkService zkService, String topologyName, int waitTime){
        String opResult = DataPullConstants.TopoStopStatus.KILLING_FAILED;
        LOG.info("开始停止topologyName为{}的进程...",topologyName);
        if(StringUtils.isBlank(topologyName)){
            LOG.info("topologyName为空,无法进行停止操作！");
            return opResult;
        }
        String stormUIAddr = getStormUIAddr(zkService,topologyName);
        if(stormUIAddr==null){
            LOG.info("无法从zookeeper上获取storm ui的IP");
            return opResult;
        }
        String topoId=null;
        try {
            topoId = getTopologyId(stormUIAddr, topologyName);
            LOG.info("topologyId为：{}",topoId);
            if(StringUtils.isNotBlank(topoId)){
                String url = "http://"+stormUIAddr+"/api/v1/topology/"+topoId+"/kill/"+waitTime;
                LOG.info("the url for killing topology:{}",url);
                String resp = HttpRequest.getInstance().doPost(url, "utf-8", "");

                String killResult = JSONObject.parseObject(resp).getString(DataPullConstants.TopoStopResult.STATUS_KEY);   
                if(StringUtils.isNotBlank(killResult)&&killResult.equals(DataPullConstants.TopoStopResult.SUCCESS)){
                    LOG.info("停止topologyName为{}的进程已结束，停止结果为{}",topologyName,resp);
                    opResult=DataPullConstants.TopoStopStatus.KILLED;
                    return opResult;
                }
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error(e.getMessage(),e);
        }
        return opResult;
    }

    private static String getStormUIAddr(ZkService zkService,String topologyName){
        if(zkService==null){
            return null;
        }
        String stormUIAddr=null;
        String zkPath = Constants.TOPOLOGY_ROOT+"/"+topologyName+"/"+Constants.ZkTopoConfForFullPull.COMMON_CONFIG+".properties";
        try {
            Properties props = zkService.getProperties(zkPath);
            stormUIAddr = props.getProperty(DataPullConstants.STORM_UI);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error(e.getMessage(),e);
        }
        return stormUIAddr;
    }

    private static String getTopologyId(String stormUIAddr, String topologyName){
        String url = "http://"+stormUIAddr+"/api/v1/topology/summary";
        List<TopologySummary> lstTopo = new ArrayList<TopologySummary>();
        String topoId = null;
        try{
            String resp =HttpRequest.getInstance().doGet(url, "utf-8");
            if(StringUtils.isNotBlank(resp)){
                ObjectMapper mapper = JsonUtil.getObjectMapper();
                JsonNode node = mapper.readTree(resp);
                JsonNode topoNode = node.get("topologies");
                if(topoNode!=null){
                    String topoStr = topoNode.toString();
                    if(StringUtils.isNotBlank(topoStr)){
                        JavaType javaType = getCollectionType(ArrayList.class, TopologySummary.class);
                        lstTopo = mapper.readValue(topoStr, javaType);
                    }
                }
            }
        }catch(Exception e){
            LOG.error(e.getMessage(),e);
        }
        for(TopologySummary topoSumm : lstTopo){
            if(topoSumm.getTopologyName()!=null && topoSumm.getTopologyName().equals(topologyName)){
                topoId = topoSumm.getTopologyId();
                break;
            }
        }
        return topoId;
    }

    private static JavaType getCollectionType(Class<?> collectionClass, Class<?>... elementClasses) {
        ObjectMapper mapper = JsonUtil.getObjectMapper();
        return mapper.getTypeFactory().constructParametricType(collectionClass, elementClasses);
    }
}
