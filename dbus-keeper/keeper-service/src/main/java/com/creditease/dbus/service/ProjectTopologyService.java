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

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.ControlMessage;
import com.creditease.dbus.commons.ControlType;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.domain.mapper.NameAliasMappingMapper;
import com.creditease.dbus.domain.mapper.ProjectMapper;
import com.creditease.dbus.domain.mapper.ProjectTopologyMapper;
import com.creditease.dbus.domain.model.NameAliasMapping;
import com.creditease.dbus.domain.model.Project;
import com.creditease.dbus.domain.model.ProjectTopology;
import com.creditease.dbus.domain.model.TopologyJar;
import com.creditease.dbus.utils.SecurityConfProvider;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

import static com.creditease.dbus.commons.Constants.ROUTER;
import static com.creditease.dbus.constant.KeeperConstants.GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS;


/**
 * Created by mal on 2018/4/12.
 */
@Service
public class ProjectTopologyService {

    private static Logger logger = LoggerFactory.getLogger(ProjectTopologyService.class);

    @Autowired
    private ProjectTopologyMapper mapper;

    @Autowired
    private ProjectMapper projectMapper;

    @Autowired
    private NameAliasMappingMapper nameAliasMappingMapper;

    @Autowired
    private IZkService zkService;

    @Autowired
    private JarManagerService jarManagerService;

    public int existTopoName(String topoName) {
        return mapper.selectCountByTopoName(topoName);
    }

    public int insert(ProjectTopology record) {
        mapper.insert(record);
        Project project = projectMapper.selectByPrimaryKey(record.getProjectId());
        generateTopologyConfig(project.getProjectName(), record.getTopoName(), record.getTopoConfig());
        generateTopologyOthersConfig(project.getProjectName(), record);

        // 为router迁移表扩容,增加拓扑名的映射别名
        NameAliasMapping nam = new NameAliasMapping();
        nam.setNameId(record.getId());
        nam.setName(record.getTopoName());
        nam.setAlias(record.getAlias());
        // 1代表router类型别名
        nam.setType(NameAliasMapping.routerType);
        nameAliasMappingMapper.insert(nam);

        return record.getId();
    }

    private void generateTopologyConfig(String projectCode, String topologyCode, String strTopoConf) {
        StringReader sr = null;
        BufferedReader br = null;
        ByteArrayOutputStream bros = null;
        OutputStreamWriter osw = null;
        BufferedWriter bw = null;
        try {
            sr = new StringReader(strTopoConf);
            br = new BufferedReader(sr);

            bros = new ByteArrayOutputStream(strTopoConf.getBytes().length);
            osw = new OutputStreamWriter(bros);
            bw = new BufferedWriter(osw);
            String line = br.readLine();
            while (line != null) {
                if (StringUtils.contains(line, "placeholder"))
                    line = StringUtils.replace(line, "placeholder", topologyCode);
                bw.write(line);
                bw.newLine();
                line = br.readLine();
            }
            bw.flush();

            String path = StringUtils.joinWith("/", Constants.ROUTER_ROOT, projectCode, topologyCode + "-" + Constants.ROUTER, "config.properties");
            if (!zkService.isExists(path)) {
                zkService.createNode(path, bros.toByteArray());
            } else {
                zkService.setData(path, bros.toByteArray());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(bw);
            IOUtils.closeQuietly(osw);
            IOUtils.closeQuietly(bros);
            IOUtils.closeQuietly(br);
            IOUtils.closeQuietly(sr);
        }
    }

    private void generateTopologyOthersConfig(String projectCode, ProjectTopology record) {
        ByteArrayInputStream bais = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        ByteArrayOutputStream bros = null;
        OutputStreamWriter osw = null;
        BufferedWriter bw = null;
        try {
            String[] names = {"consumer.properties", "producer.properties"};
            for (String name : names) {
                String srcPath = StringUtils.joinWith("/", Constants.DBUS_CONF_TEMPLATE_ROOT, "Router/placeholder-router", name);
                byte[] data = zkService.getData(srcPath);
                bais = new ByteArrayInputStream(data);
                isr = new InputStreamReader(bais);
                br = new BufferedReader(isr);

                bros = new ByteArrayOutputStream(data.length);
                osw = new OutputStreamWriter(bros);
                bw = new BufferedWriter(osw);
                String line = br.readLine();
                while (line != null) {
                    if (StringUtils.contains(line, "placeholder"))
                        line = StringUtils.replace(line, "placeholder", record.getTopoName());
                    bw.write(line);
                    bw.newLine();
                    line = br.readLine();
                }
                bw.flush();
                String destPath = StringUtils.joinWith("/", Constants.ROUTER_ROOT, projectCode, record.getTopoName() + "-" + Constants.ROUTER, name);
                if (!zkService.isExists(destPath)) {
                    zkService.createNode(destPath, bros.toByteArray());
                } else {
                    zkService.setData(destPath, bros.toByteArray());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(bw);
            IOUtils.closeQuietly(osw);
            IOUtils.closeQuietly(bros);
            IOUtils.closeQuietly(br);
            IOUtils.closeQuietly(isr);
            IOUtils.closeQuietly(bais);
        }
    }

    public String obtainRouterTopologyConfigTemplate() {
        String strTopoConf = StringUtils.EMPTY;
        ByteArrayInputStream bais = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        ByteArrayOutputStream bros = null;
        OutputStreamWriter osw = null;
        BufferedWriter bw = null;
        try {
            String path = StringUtils.joinWith("/", Constants.DBUS_CONF_TEMPLATE_ROOT, "Router/placeholder-router", "config.properties");
            byte[] data = zkService.getData(path);
            bais = new ByteArrayInputStream(data);
            isr = new InputStreamReader(bais);
            br = new BufferedReader(isr);

            bros = new ByteArrayOutputStream(data.length);
            osw = new OutputStreamWriter(bros);
            bw = new BufferedWriter(osw);
            String line = br.readLine();
            while (line != null) {
                bw.write(line);
                bw.newLine();
                line = br.readLine();
            }
            bw.flush();
            strTopoConf = bros.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(bw);
            IOUtils.closeQuietly(osw);
            IOUtils.closeQuietly(bros);
            IOUtils.closeQuietly(br);
            IOUtils.closeQuietly(isr);
            IOUtils.closeQuietly(bais);
        }
        return strTopoConf;
    }

    public int update(ProjectTopology record) {
        if (StringUtils.isNoneBlank(record.getTopoName())) {
            Map<String, Object> param = new HashMap<>();
            param.put("nameId", record.getId());
            param.put("name", record.getTopoName());
            param.put("type", 1);
            NameAliasMapping nam = nameAliasMappingMapper.selectByCondition(param);
            if (nam == null || nam.getId() == null) {
                nam = new NameAliasMapping();
                nam.setNameId(record.getId());
                nam.setName(record.getTopoName());
                nam.setAlias(record.getAlias());
                nam.setType(NameAliasMapping.routerType);
                nameAliasMappingMapper.insert(nam);
            } else {
                nam.setAlias(record.getAlias());
                nam.setUpdateTime(new Date());
                nameAliasMappingMapper.updateByPrimaryKey(nam);
            }
        }
        record.setUpdateTime(new Date());
        return mapper.updateByPrimaryKey(record);
    }

    public ProjectTopology select(Integer topoId) {
        ProjectTopology pt = mapper.selectByPrimaryKey(topoId);
        if (pt != null && pt.getId() != null) {
            Map<String, Object> param = new HashMap<>();
            param.put("nameId", pt.getId());
            param.put("name", pt.getTopoName());
            param.put("type", 1);
            NameAliasMapping nam = nameAliasMappingMapper.selectByCondition(param);
            if (nam != null) {
                pt.setAlias(nam.getAlias());
            }
        }
        return pt;
    }

    public int delete(Integer projectId) {

        ProjectTopology pt = this.select(projectId);
        if (pt != null) {
            Project project = projectMapper.selectByPrimaryKey(pt.getProjectId());
            if (project != null) {
                deleteZkConf(project.getProjectName(), pt.getTopoName());
            }
        }

        // 删除相应别名映射
        Map<String, Object> param = new HashMap<>();
        param.put("nameId", pt.getId());
        param.put("name", pt.getTopoName());
        param.put("type", 1);
        NameAliasMapping nam = nameAliasMappingMapper.selectByCondition(param);
        if (nam != null && nam.getId() != null) {
            nameAliasMappingMapper.deleteByPrimaryKey(nam.getId());
        }

        return mapper.deleteByPrimaryKey(projectId);

    }

    private void deleteZkConf(String projectCode, String topologyName) {
        try {
            String destPath = StringUtils.joinWith("/", Constants.ROUTER_ROOT, projectCode, topologyName + "-" + Constants.ROUTER);
            if (zkService.isExists(destPath)) {
                zkService.rmr(destPath);
                logger.info("delete router config:{}", destPath);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> queryJarVersions() throws Exception {
        return jarManagerService.queryVersion("router");
    }

    public List<String> queryJarPackages(String version) throws Exception {
        return jarManagerService.queryJarInfos("router", version, "router").stream().map(TopologyJar::getPath).collect(Collectors.toList());
    }

    public PageInfo search(Integer projectId,
                           String topoName,
                           Integer pageNum,
                           Integer pageSize,
                           String sortby,
                           String order) {
        Map<String, Object> map = new HashMap<>();
        map.put("projectId", projectId);
        map.put("topoName", topoName);
        map.put("sortby", sortby);
        map.put("order", order);
        PageHelper.startPage(pageNum, pageSize);
        List<Map<String, Object>> users = mapper.search(map);
        // 分页结果
        PageInfo<List<ProjectTopology>> page = new PageInfo(users);
        return page;
    }

    public List<Map<String, Object>> queryOutPutTopics(Integer projectId, Integer topoId) {
        Map<String, Object> map = new HashMap<>();
        map.put("projectId", projectId);
        map.put("topoId", topoId);
        return mapper.selectOutPutTopics(map);
    }

    public List<Map<String, Object>> rerunInit(Integer projectId, Integer topoId) {
        KafkaConsumer<String, byte[]> consumer = null;
        List<Map<String, Object>> list = new ArrayList<>();
        try {
            Properties props = zkService.getProperties(KeeperConstants.KEEPER_CONSUMER_CONF);
            props.put("group.id", StringUtils.joinWith(".", props.getProperty("group.id"), "pts"));
            props.put("client.id", StringUtils.joinWith(".", props.getProperty("client.id"), "pts"));
            if (StringUtils.equals(SecurityConfProvider.getSecurityConf(zkService), Constants.SECURITY_CONFIG_TRUE_VALUE)) {
                props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            }
            consumer = new KafkaConsumer<String, byte[]>(props);
            List<Map<String, Object>> inTopics = this.queryInPutTopics(projectId, topoId);
            // outputTopic
            List<TopicPartition> topics = new ArrayList<>();
            for (Map<String, Object> item : inTopics) {
                List<PartitionInfo> partitionInfos = consumer.partitionsFor((String) item.get("outputTopic"));
                for (PartitionInfo pif : partitionInfos) {
                    TopicPartition tp = new TopicPartition(pif.topic(), pif.partition());
                    topics.add(tp);
                }
            }
            consumer.assign(topics);
            consumer.seekToEnd(topics);
            for (Map<String, Object> item : inTopics) {
                List<PartitionInfo> partitionInfos = consumer.partitionsFor((String) item.get("outputTopic"));
                for (PartitionInfo pif : partitionInfos) {
                    Map<String, Object> map = new HashMap<>();
                    TopicPartition tp = new TopicPartition(pif.topic(), pif.partition());
                    map.put("topic", pif.topic());
                    map.put("partition", pif.partition());
                    map.put("latestOffset", consumer.position(tp));
                    map.remove("status");
                    /*if (StringUtils.contains((String) item.get("status"), "running") ||
                        StringUtils.contains((String) item.get("status"), "changed"))
                    map.put("isCanRerun", true);
                    else map.put("isCanRerun", false);*/
                    list.add(map);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (consumer != null) consumer.close();
        }
        return list;
    }

    public void rerunTopology(String topologyCode, String ctrlMsg) {
        KafkaProducer<String, byte[]> producer = null;
        try {
            String topic = StringUtils.joinWith("_", topologyCode, "ctrl");
            Properties props = zkService.getProperties(KeeperConstants.KEEPER_CTLMSG_PRODUCER_CONF);
            Properties globalConf = zkService.getProperties(KeeperConstants.GLOBAL_CONF);
            props.setProperty(GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS, globalConf.getProperty(GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS));
            if (StringUtils.equals(SecurityConfProvider.getSecurityConf(zkService), Constants.SECURITY_CONFIG_TRUE_VALUE)) {
                props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            }
            producer = new KafkaProducer<>(props);
            producer.send(new ProducerRecord<String, byte[]>(topic, ctrlMsg.getBytes()), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (producer != null) producer.close();
        }
    }

    public void effectTopologyConfig(Integer topologyId) {
        StringReader sr = null;
        KafkaProducer<String, byte[]> producer = null;
        try {
            ProjectTopology pt = this.select(topologyId);
            String topologyCode = pt.getTopoName();
            String strTopoConfig = pt.getTopoConfig();

            Project project = projectMapper.selectByPrimaryKey(pt.getProjectId());
            String projectCode = project.getProjectName();

            sr = new StringReader(strTopoConfig);
            Properties newConfig = new Properties();
            newConfig.load(sr);

            String path = StringUtils.joinWith("/", Constants.ROUTER_ROOT, projectCode, topologyCode + "-router", "config.properties");
            Properties oldConfig = zkService.getProperties(path);

            // 一定要加载完成旧的配置再去保存新的配置
            this.saveTopologyConfig(projectCode, topologyCode, strTopoConfig);

            Properties props = zkService.getProperties(KeeperConstants.KEEPER_CTLMSG_PRODUCER_CONF);
            Properties globalConf = zkService.getProperties(KeeperConstants.GLOBAL_CONF);
            props.setProperty(GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS, globalConf.getProperty(GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS));
            if (StringUtils.equals(SecurityConfProvider.getSecurityConf(zkService), Constants.SECURITY_CONFIG_TRUE_VALUE)) {
                props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            }
            producer = new KafkaProducer<>(props);

            // 发送重跑控制信息
            if (!StringUtils.equals(newConfig.getProperty("router.topic.offset"), oldConfig.getProperty("router.topic.offset"))) {
                ControlMessage ctrlMsg = new ControlMessage(System.currentTimeMillis(), ControlType.ROUTER_TOPOLOGY_RERUN.name().toUpperCase(), "dbus-web");
                Map<String, Object> payload = new HashMap<>();
                payload.put("projectTopoId", pt.getId());
                payload.put("isFromEffect", true);
                payload.put("offset", newConfig.getProperty("router.topic.offset"));
                ctrlMsg.setPayload(payload);
                String topic = StringUtils.joinWith("_", topologyCode, "ctrl");
                String data = ctrlMsg.toJSONString();
                producer.send(new ProducerRecord<String, byte[]>(topic, data.getBytes()), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                    }
                });
            }

            // 发送生效控制信息
//            if (!StringUtils.equals(newConfig.getProperty("stat.topic"), oldConfig.getProperty("stat.topic")) ||
//                    !StringUtils.equals(newConfig.getProperty("stat.url"), oldConfig.getProperty("stat.url"))) {
            ControlMessage ctrlMsg = new ControlMessage(System.currentTimeMillis(), ControlType.ROUTER_TOPOLOGY_EFFECT.name().toUpperCase(), "dbus-web");
            Map<String, Object> payload = new HashMap<>();
            payload.put("projectTopoId", pt.getId());
            ctrlMsg.setPayload(payload);
            String topic = StringUtils.joinWith("_", topologyCode, "ctrl");
            String data = ctrlMsg.toJSONString();
            producer.send(new ProducerRecord<String, byte[]>(topic, data.getBytes()), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                }
            });
//            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(sr);
            if (producer != null) producer.close();
        }
    }

    private void saveTopologyConfig(String projectCode, String topologyCode, String strTopoConf) throws Exception {
        StringReader sr = null;
        BufferedReader br = null;
        ByteArrayOutputStream bros = null;
        OutputStreamWriter osw = null;
        BufferedWriter bw = null;
        try {
            sr = new StringReader(strTopoConf);
            br = new BufferedReader(sr);

            bros = new ByteArrayOutputStream(strTopoConf.getBytes().length);
            osw = new OutputStreamWriter(bros);
            bw = new BufferedWriter(osw);
            String line = br.readLine();
            while (line != null) {
                if (StringUtils.contains(line, "router.topic.offset")) {
                    String arrs[] = StringUtils.split(line, "=");
                    line = StringUtils.replace(line, arrs[1], "none");
                }
                bw.write(line);
                bw.newLine();
                line = br.readLine();
            }
            bw.flush();
            String path = Constants.ROUTER_ROOT + "/" + projectCode + "/" + topologyCode + "-" + ROUTER + "/config.properties";
            zkService.setData(path, bros.toByteArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(bw);
            IOUtils.closeQuietly(osw);
            IOUtils.closeQuietly(bros);
            IOUtils.closeQuietly(br);
            IOUtils.closeQuietly(sr);
        }
    }

    public List<Map<String, Object>> queryInPutTopics(Integer projectId, Integer topoId) {
        Map<String, Object> map = new HashMap<>();
        map.put("projectId", projectId);
        map.put("topoId", topoId);
        return mapper.selectInPutTopics(map);
    }

    public List<NameAliasMapping> getTopoAlias(Integer topoId) {
        return nameAliasMappingMapper.getTargetTopoByTopoId(topoId);
    }

    public List<Map<String, Object>> selectByIds(List<Integer> ids) {
        return mapper.selectByIds(ids);
    }

}
