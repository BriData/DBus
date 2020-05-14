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
import com.creditease.dbus.domain.model.SinkerTopology;
import com.creditease.dbus.domain.model.SinkerTopologySchema;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class SinkerService {
    private Logger logger = LoggerFactory.getLogger(getClass());
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
    @Autowired
    private SinkerSchemaService sinkerSchemaService;
    @Autowired
    private DataSourceService dataSourceService;

    public ResultEntity search(String queryString) throws Exception {
        ResultEntity body = sender.get(ServiceNames.KEEPER_SERVICE, "/sinker/search", queryString).getBody();
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

    public ResultEntity create(SinkerTopology sinkerTopology) throws Exception {
        ResultEntity body = sender.post(ServiceNames.KEEPER_SERVICE, "/sinker/create", sinkerTopology).getBody();
        if (body.getStatus() == ResultEntity.SUCCESS) {
            zkConfService.cloneSinkerConfFromTemplate("sinker", sinkerTopology.getSinkerName());
        }
        return body;
    }

    public ResultEntity update(SinkerTopology sinkerTopology) throws Exception {
        sinkerTopology.setStatus("changed");
        ResultEntity body = updateSinkerTopology(sinkerTopology);
        String sinkerConf = sinkerTopology.getSinkerConf();
        zkService.setData(Constants.SINKER_ROOT + "/" + sinkerTopology.getSinkerName() + "-sinker/config.properties", sinkerConf.getBytes("utf-8"));
        return body;
    }

    private ResultEntity updateSinkerTopology(SinkerTopology sinkerTopology) {
        return sender.post(ServiceNames.KEEPER_SERVICE, "/sinker/update", sinkerTopology).getBody();
    }

    public ResultEntity delete(Integer id) throws Exception {
        SinkerTopology topology = getById(id);
        ResultEntity body = sender.get(ServiceNames.KEEPER_SERVICE, "/sinker/delete/{0}", id).getBody();
        if (body.getStatus() == ResultEntity.SUCCESS) {
            zkConfService.deleteZkNodeOfPath(Constants.SINKER_ROOT + "/" + topology.getSinkerName() + "-sinker");
        }
        return body;
    }

    public String startSinkerTopology(Map<String, Object> param) throws Exception {

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
        SinkerTopology sinkerTopology = new SinkerTopology();
        sinkerTopology.setId((Integer) param.get("id"));
        sinkerTopology.setStatus("running");
        updateSinkerTopology(sinkerTopology);
        return result;
    }

    public ResultEntity stopSinkerTopology(Map<String, Object> param) throws Exception {
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
        SinkerTopology sinkerTopology = new SinkerTopology();
        sinkerTopology.setId((Integer) param.get("id"));
        sinkerTopology.setStatus("stopped");
        updateSinkerTopology(sinkerTopology);
        return resultEntity;
    }

    public int reload(SinkerTopology sinkerTopology) {
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
        logger.info("send reload sinker message success.{}", JSON.toJSONString(sinkerTopology));
        return toolSetService.reloadSinkerConfig(ctrlTopic, json.toJSONString());
    }

    public SinkerTopology getById(Integer id) {
        return sender.get(ServiceNames.KEEPER_SERVICE, "/sinker/searchById/{0}", id).getBody().getPayload(SinkerTopology.class);
    }

    public SinkerTopology searchById(Integer id) throws Exception {
        SinkerTopology sinkerTopology = getById(id);
        byte[] data = zkService.getData(Constants.SINKER_ROOT + "/" + sinkerTopology.getSinkerName() + "-sinker/config.properties");
        sinkerTopology.setSinkerConf(new String(data));
        return sinkerTopology;
    }

    public Collection<Map<String, Object>> getSinkerTopicInfos(String sinkerName) {
        KafkaConsumer<String, byte[]> consumer = null;
        try {
            List<SinkerTopologySchema> sinkerTopologySchemas = sinkerSchemaService.getBySinkerName(sinkerName);
            Properties props = zkService.getProperties(KeeperConstants.KEEPER_CONSUMER_CONF);
            props.put("group.id", props.getProperty("group.id") + ".sinker");
            props.put("client.id", props.getProperty("client.id") + ".sinker");
            if (StringUtils.equals(SecurityConfProvider.getSecurityConf(zkService), Constants.SECURITY_CONFIG_TRUE_VALUE)) {
                props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            }
            consumer = new KafkaConsumer<>(props);
            // outputTopic
            List<TopicPartition> topicPartitions = new ArrayList<>();
            HashMap<String, List<PartitionInfo>> partitionInfoMap = new HashMap<>();
            for (SinkerTopologySchema sinkerTopologySchema : sinkerTopologySchemas) {
                List<PartitionInfo> partitionInfos = consumer.partitionsFor(sinkerTopologySchema.getTargetTopic());
                partitionInfoMap.put(sinkerTopologySchema.getTargetTopic(), partitionInfos);
                for (PartitionInfo partitionInfo : partitionInfos) {
                    TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
                    topicPartitions.add(topicPartition);
                }
            }

            Map<String, Map<String, Object>> result = new HashMap<>();
            consumer.assign(topicPartitions);
            consumer.seekToBeginning(topicPartitions);
            for (SinkerTopologySchema sinkerTopologySchema : sinkerTopologySchemas) {
                List<PartitionInfo> partitionInfos = partitionInfoMap.get(sinkerTopologySchema.getTargetTopic());
                for (PartitionInfo info : partitionInfos) {
                    Map<String, Object> map = new HashMap<>();
                    TopicPartition tp = new TopicPartition(info.topic(), info.partition());
                    map.put("topic", info.topic());
                    map.put("partition", info.partition());
                    map.put("beginOffset", consumer.position(tp));
                    result.put(sinkerTopologySchema.getTargetTopic() + "&" + info.partition(), map);
                }
            }
            consumer.seekToEnd(topicPartitions);
            for (SinkerTopologySchema sinkerTopologySchema : sinkerTopologySchemas) {
                List<PartitionInfo> partitionInfos = partitionInfoMap.get(sinkerTopologySchema.getTargetTopic());
                for (PartitionInfo pif : partitionInfos) {
                    TopicPartition info = new TopicPartition(pif.topic(), pif.partition());
                    Map<String, Object> map = result.get(sinkerTopologySchema.getTargetTopic() + "&" + info.partition());
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

    public void addSinkerSchemas(Map<String, Object> param) {
        SinkerTopology topology = JSON.parseObject(JSON.toJSONString(param.get("sinkerTopology")), SinkerTopology.class);
        List<SinkerTopologySchema> sinkerSchemaList = JSON.parseArray(JSON.toJSONString(param.get("sinkerSchemaList")), SinkerTopologySchema.class);
        List<SinkerTopologySchema> addSchemas = sinkerSchemaList.stream()
                .filter(schema -> schema.getSinkerTopoId() == null)
                .map(schema -> {
                    schema.setSinkerName(topology.getSinkerName());
                    schema.setSinkerTopoId(topology.getId());
                    return schema;
                }).collect(Collectors.toList());
        // 更新到数据库
        if (!sinkerSchemaList.isEmpty()) {
            // 同一个topic只能在一个sinker中消费
            Integer sinkerTopoId = sinkerSchemaList.get(0).getSinkerTopoId();
            List<SinkerTopologySchema> list = sinkerSchemaService.selectAll();
            Set<String> topics = list.stream().filter(schema -> schema.getSinkerTopoId() != sinkerTopoId).map(SinkerTopologySchema::getTargetTopic).collect(Collectors.toSet());
            for (SinkerTopologySchema schema : sinkerSchemaList) {
                if (topics.contains(schema.getTargetTopic())) {
                    throw new RuntimeException(String.format("Topic %s 只能在一个sinker消费.", schema.getTargetTopic()));
                }
            }
            // 添加schama
            int count = sinkerSchemaService.addSinkerSchemas(addSchemas);
            if (addSchemas.size() != count) {
                throw new RuntimeException("添加sinker Schema失败.");
            }
            // 添加table
            List<SinkerTopologySchema> addSchemaTables = addSchemas.stream().filter(schema -> schema.getAddAllTable() == null || schema.getAddAllTable()).collect(Collectors.toList());
            if (addSchemaTables != null && !addSchemaTables.isEmpty()) {
                sinkerSchemaService.batchAddSinkerTables(addSchemaTables);
            }
        }
    }

    public Map viewLog(String sinkerName) throws Exception {
        List<Topology> topologies = stormTopoHelper.getAllTopologiesInfo();
        List<Topology> list = topologies.stream().filter(topology -> StringUtils.equals(topology.getName(), sinkerName + "-sinker")).collect(Collectors.toList());
        if (list == null || list.isEmpty()) {
            return new HashMap<>();
        }
        Topology topology = list.get(0);
        return dataSourceService.viewLog(topology.getId());
    }
}
