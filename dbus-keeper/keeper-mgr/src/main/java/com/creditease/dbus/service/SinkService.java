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


package com.creditease.dbus.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.base.com.creditease.dbus.utils.RequestSender;
import com.creditease.dbus.bean.Topology;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.ControlType;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.constant.ServiceNames;
import com.creditease.dbus.domain.model.Sink;
import com.creditease.dbus.domain.model.SinkerTopology;
import com.creditease.dbus.domain.model.SinkerTopologySchema;
import com.creditease.dbus.utils.OrderedProperties;
import com.creditease.dbus.utils.SSHUtils;
import com.creditease.dbus.utils.SecurityConfProvider;
import com.creditease.dbus.utils.StormToplogyOpHelper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.github.pagehelper.PageInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by xiancangao on 2018/3/28.
 */
@Service
public class SinkService {
    @Autowired
    private RequestSender sender;
    @Autowired
    private IZkService zkService;
    @Autowired
    private Environment env;
    @Autowired
    private ProjectTopologyService projectTopologyService;
    @Autowired
    private StormToplogyOpHelper stormTopoHelper;
    @Autowired
    private ToolSetService toolSetService;
    @Autowired
    private ZkConfService zkConfService;

    protected Logger logger = LoggerFactory.getLogger(getClass());

    public ResultEntity createSink(Sink sink) {
        ResponseEntity<ResultEntity> result = sender.post(ServiceNames.KEEPER_SERVICE, "/sinks/create", sink);
        return result.getBody();
    }

    public ResultEntity updateSink(Sink sink) {
        ResponseEntity<ResultEntity> result = sender.post(ServiceNames.KEEPER_SERVICE, "/sinks/update", sink);
        return result.getBody();
    }

    public ResultEntity deleteSink(Integer id) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/sinks/delete/{0}", id);
        return result.getBody();
    }

    public ResultEntity search(String queryString) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/sinks/search", queryString);
        return result.getBody();
    }

    public boolean sinkTest(String url) {
        String[] urls = url.split(",");
        Socket socket = null;
        try {
            for (String s : urls) {
                socket = new Socket();
                String[] ipPort = s.split(":");
                if (ipPort == null || ipPort.length != 2) {
                    return false;
                }
                socket.connect(new InetSocketAddress(ipPort[0], Integer.parseInt(ipPort[1])));
                socket.close();
            }
            logger.info("sink连通性测试通过.{}", url);
            return true;
        } catch (IOException e) {
            logger.error("sink连通性测试异常.==={};==={}", url, e.getMessage(), e);
            return false;
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    public ResultEntity searchByUserProject(String queryString) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/sinks/search-by-user-project", queryString);
        return result.getBody();
    }

    public ResultEntity searchSinkerTopology(String queryString) throws Exception {
        ResultEntity body = sender.get(ServiceNames.KEEPER_SERVICE, "/sinks/searchSinkerTopology", queryString).getBody();
        PageInfo<Map<String, Object>> sinkerTopologyList = body.getPayload(new TypeReference<PageInfo<Map<String, Object>>>() {
        });
        List<Map<String, Object>> list = sinkerTopologyList.getList();
        if (list != null && !list.isEmpty()) {
            List<Topology> topologies = stormTopoHelper.getAllTopologiesInfo();
            for (Map<String, Object> sinker : list) {
                String status = correctStatus(topologies, (String) sinker.get("status"), (String) sinker.get("sinkerName"));
                sinker.put("status", status);
            }
        }

        body.setPayload(sinkerTopologyList);
        return body;
    }

    public String correctStatus(List<Topology> topologies, String originStatus, String topologyName) {
        String stormStatus = "stopped";
        for (Topology topology : topologies) {
            if (StringUtils.equals(topology.getName(), topologyName + "-sinker")
                    && StringUtils.equals(topology.getStatus(), "ACTIVE")) {
                stormStatus = "running";
            }
        }
        return projectTopologyService.getTopoStatus(originStatus, stormStatus);
    }

    public String startSinkerTopology(Map<String, Object> param) throws Exception {
        SinkerTopology sinkerTopology = new SinkerTopology();
        BeanUtils.copyProperties(param, sinkerTopology);
        Properties globalConf = zkService.getProperties(KeeperConstants.GLOBAL_CONF);

        String hostIp = globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOST);
        String port = globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_PORT);
        String stormBaseDir = globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOME_PATH);
        String stormSshUser = globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_USER);
        String sinkerName = (String) param.get("sinkerName");
        String jarPath = (String) param.get("jarPath");
        String cmd = String.format("cd %s/%s;./dbus_startTopology.sh %s %s %s %s %s", stormBaseDir, KeeperConstants.STORM_JAR_DIR,
                stormBaseDir, "sinker", env.getProperty("zk.str"), sinkerName, jarPath);
        logger.info("Topology Start Command:{}", cmd);
        String result = SSHUtils.executeCommand(stormSshUser, hostIp, Integer.parseInt(port), env.getProperty("pubKeyPath"), cmd, null);
        Topology topology = stormTopoHelper.getTopologyByName(sinkerName + "-sinker");
        String status = topology == null ? sinkerTopology.getStatus() : topology.getStatus();
        sinkerTopology.setStatus(status);
        updateSinkerTopology(sinkerTopology);
        return result;
    }

    public ResultEntity stopSinkerTopology(Map<String, Object> param) throws Exception {
        SinkerTopology sinkerTopology = new SinkerTopology();
        BeanUtils.copyProperties(param, sinkerTopology);

        String sinkerName = (String) param.get("sinkerName");
        List<Topology> topologies = stormTopoHelper.getAllTopologiesInfo();
        String topologyId = null;
        for (Topology topology : topologies) {
            if (StringUtils.equals(topology.getName(), sinkerName + "-sinker")
                    && StringUtils.equals(topology.getStatus(), "ACTIVE")) {
                topologyId = topology.getId();
            }
        }
        ResultEntity resultEntity = new ResultEntity();
        if (topologyId == null) {
            resultEntity.setStatus(MessageCode.EXCEPTION);
            resultEntity.setMessage("sinker topology 不存在");
            resultEntity.setPayload("sinker topology 不存在");
            return resultEntity;
        }
        String killResult = stormTopoHelper.stopTopology(topologyId, 10, env.getProperty("pubKeyPath"));
        if (!"ok".equals(killResult)) {
            resultEntity.setStatus(MessageCode.EXCEPTION);
            resultEntity.setMessage(killResult);
            resultEntity.setPayload(killResult);
        } else {
            resultEntity.setPayload("kill sinker success.");
        }
        Topology topology = stormTopoHelper.getTopologyByName(sinkerName + "-sinker");
        String status = topology == null ? sinkerTopology.getStatus() : topology.getStatus();
        sinkerTopology.setStatus(status);
        updateSinkerTopology(sinkerTopology);
        return resultEntity;
    }

    public ResultEntity createSinkerTopology(SinkerTopology sinkerTopology) throws Exception {
        ResultEntity body = sender.post(ServiceNames.KEEPER_SERVICE, "/sinks/createSinkerTopology", sinkerTopology).getBody();
        if (body.getStatus() == ResultEntity.SUCCESS) {
            zkConfService.cloneSinkerConfFromTemplate("sinker", sinkerTopology.getSinkerName());
        }
        return body;
    }

    public ResultEntity updateSinkerTopology(SinkerTopology sinkerTopology) {
        return sender.post(ServiceNames.KEEPER_SERVICE, "/sinks/updateSinkerTopology", sinkerTopology).getBody();
    }

    public ResultEntity deleteSinkerTopology(Integer id) throws Exception {
        SinkerTopology topology = searchSinkerTopologyById(id);
        ResultEntity body = sender.get(ServiceNames.KEEPER_SERVICE, "/sinks/deleteSinkerTopology/{0}", id).getBody();
        if (body.getStatus() == ResultEntity.SUCCESS) {
            zkConfService.deleteZkNodeOfPath(Constants.SINKER_ROOT + "/" + topology.getSinkerName() + "-sinker");
        }
        return body;
    }

    public int reloadSinkerTopology(SinkerTopology sinkerTopology) {
        String ctrlTopic = sinkerTopology.getSinkerName() + "_sinker_ctrl";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        JSONObject payload = new JSONObject();
        payload.put("sinkerName", sinkerTopology.getSinkerName());
        JSONObject json = new JSONObject();
        json.put("from", "dbus-web");
        json.put("id", System.currentTimeMillis());
        json.put("payload", payload);
        json.put("timestamp", sdf.format(new Date()));
        json.put("type", ControlType.SINKER_RELOAD_CONFIG.name());
        return toolSetService.reloadSinkerConfig(ctrlTopic, json.toJSONString());
    }

    public SinkerTopology searchSinkerTopologyById(Integer id) {
        return sender.get(ServiceNames.KEEPER_SERVICE, "/sinks/searchSinkerTopologyById/{0}", id).getBody().getPayload(SinkerTopology.class);
    }

    public Collection<Map<String, Object>> getSinkerTopicInfos(String sinkerName) {
        KafkaConsumer<String, byte[]> consumer = null;
        try {
            Properties properties = zkService.getProperties(Constants.SINKER_ROOT + "/" + sinkerName + "-sinker/config.properties");
            String sinkerTopic = properties.getProperty("sinker.topic.list");
            String[] topics = StringUtils.split(sinkerTopic, ",");

            Properties props = zkService.getProperties(KeeperConstants.KEEPER_CONSUMER_CONF);
            props.put("group.id", props.getProperty("group.id") + ".sinker");
            props.put("client.id", props.getProperty("client.id") + ".sinker");
            if (StringUtils.equals(SecurityConfProvider.getSecurityConf(zkService), Constants.SECURITY_CONFIG_TRUE_VALUE)) {
                props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            }
            consumer = new KafkaConsumer<String, byte[]>(props);
            // outputTopic
            List<TopicPartition> topicPartitions = new ArrayList<>();
            HashMap<String, List<PartitionInfo>> partitionInfoMap = new HashMap<>();
            for (String topic : topics) {
                List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
                partitionInfoMap.put(topic, partitionInfos);
                for (PartitionInfo partitionInfo : partitionInfos) {
                    TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
                    topicPartitions.add(topicPartition);
                }
            }

            Map<String, Map<String, Object>> result = new HashMap<>();
            consumer.assign(topicPartitions);
            consumer.seekToBeginning(topicPartitions);
            for (String topic : topics) {
                List<PartitionInfo> partitionInfos = partitionInfoMap.get(topic);
                for (PartitionInfo info : partitionInfos) {
                    Map<String, Object> map = new HashMap<>();
                    TopicPartition tp = new TopicPartition(info.topic(), info.partition());
                    map.put("topic", info.topic());
                    map.put("partition", info.partition());
                    map.put("beginOffset", consumer.position(tp));
                    result.put(topic + "&" + info.partition(), map);
                }
            }
            consumer.seekToEnd(topicPartitions);
            for (String topic : topics) {
                List<PartitionInfo> partitionInfos = partitionInfoMap.get(topic);
                for (PartitionInfo pif : partitionInfos) {
                    TopicPartition info = new TopicPartition(pif.topic(), pif.partition());
                    Map<String, Object> map = result.get(topic + "&" + info.partition());
                    map.put("endOffset", consumer.position(info));
                }
            }
            Collection<Map<String, Object>> values = result.values();
            return values;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (consumer != null) consumer.close();
        }
    }

    public int dragBackRunAgain(Map<String, Object> param) {
        String sinkerName = (String) param.get("sinkerName");

        List<Map<String, Object>> topicInfos = (List<Map<String, Object>>) param.get("topicInfo");
        ArrayList<Object> offset = new ArrayList<>();
        topicInfos.forEach(map -> {
            if (map.get("position") != null) {
                offset.add(map);
            }
        });
        if (offset.isEmpty()) {
            return 0;
        }
        JSONObject payload = new JSONObject();
        payload.put("sinkerName", sinkerName);
        payload.put("offset", offset);
        String ctrlTopic = sinkerName + "_sinker_ctrl";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        JSONObject json = new JSONObject();
        json.put("from", "dbus-web");
        json.put("id", System.currentTimeMillis());
        json.put("payload", payload);
        json.put("timestamp", sdf.format(new Date()));
        json.put("type", ControlType.SINKER_DRAG_BACK_RUN_AGAIN.name());
        return toolSetService.reloadSinkerConfig(ctrlTopic, json.toJSONString());
    }

    public ResultEntity searchSinkerTopologySchema(String request) {
        return sender.get(ServiceNames.KEEPER_SERVICE, "/sinks/searchSinkerTopologySchema", request).getBody();
    }

    public void updateSinkerTopologySchema(Map<String, Object> param) throws Exception {
        SinkerTopology topology = JSON.parseObject(JSON.toJSONString(param.get("sinkerTopology")), SinkerTopology.class);
        List<SinkerTopologySchema> sinkerSchemaList = JSON.parseArray(JSON.toJSONString(param.get("sinkerSchemaList")), SinkerTopologySchema.class);
        sinkerSchemaList.forEach(sinkerTopologySchema -> {
            sinkerTopologySchema.setSinkerName(topology.getSinkerName());
            sinkerTopologySchema.setSinkerTopoId(topology.getId());
        });
        List<Integer> schemaIds = sinkerSchemaList.stream().map(SinkerTopologySchema::getSchemaId).collect(Collectors.toList());
        List<SinkerTopologySchema> original = JSON.parseArray(JSON.toJSONString(param.get("originalSinkerSchemaList")), SinkerTopologySchema.class);
        original.forEach(schema -> {
            if (!schemaIds.contains(schema.getSchemaId())) {
                schema.setSinkerName(null);
                schema.setSinkerTopoId(null);
                sinkerSchemaList.add(schema);
            }
        });
        // 更新到数据库
        if (!sinkerSchemaList.isEmpty()) {
            sender.post(ServiceNames.KEEPER_SERVICE, "/sinks/updateSinkerTopologySchema", sinkerSchemaList).getBody();
        }
        // 更新到zk节点
        String resultTopic = sinkerSchemaList.stream().map(SinkerTopologySchema::getTargetTopic).collect(Collectors.joining(","));
        byte[] data = zkService.getData(Constants.SINKER_ROOT + "/" + topology.getSinkerName() + "-sinker/config.properties");
        OrderedProperties orderedProperties = new OrderedProperties(new String(data));
        orderedProperties.put("sinker.topic.list", resultTopic);
        zkService.setData(Constants.SINKER_ROOT + "/" + topology.getSinkerName() + "-sinker/config.properties", orderedProperties.toString().getBytes());
    }

}
