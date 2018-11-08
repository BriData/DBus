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

package com.creditease.dbus.service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.alibaba.fastjson.JSON;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.ControlMessage;
import com.creditease.dbus.commons.ControlType;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.domain.mapper.ProjectMapper;
import com.creditease.dbus.domain.mapper.ProjectTopologyMapper;
import com.creditease.dbus.domain.model.Project;
import com.creditease.dbus.domain.model.ProjectTopology;
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
    private IZkService zkService;

    public int existTopoName(String topoName) {
        return mapper.selectCountByTopoName(topoName);
    }

    public int insert(ProjectTopology record) {
        mapper.insert(record);
        Project project = projectMapper.selectByPrimaryKey(record.getProjectId());
        generateTopologyConfig(project.getProjectName(), record.getTopoName(), record.getTopoConfig());
        generateTopologyOthersConfig(project.getProjectName(), record);
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
        record.setUpdateTime(new Date());
        return mapper.updateByPrimaryKey(record);
    }

    public ProjectTopology select(Integer topoId) {
        return mapper.selectByPrimaryKey(topoId);
    }

    public int delete(Integer projectId) {
        ProjectTopology pt = this.select(projectId);
        if (pt != null) {
            Project project = projectMapper.selectByPrimaryKey(pt.getProjectId());
            if (project != null) {
                deleteZkConf(project.getProjectName(), pt.getTopoName());
            }
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
        String baseDir = obtainBaseDir();
        Properties props = zkService.getProperties(Constants.COMMON_ROOT + "/" + Constants.GLOBAL_PROPERTIES);
        String user = props.getProperty("user");
        String stormNimbusHost = props.getProperty("storm.nimbus.host");
        String port = props.getProperty("storm.nimbus.port");

        // 0:user name, 1:nimbus host, 2:ssh port,
        String cmd = MessageFormat.format("ssh {0}@{1} -p {2} find {3} -maxdepth 1 -a -type d | sed '1d'",
                user, stormNimbusHost, port, baseDir) + " | awk -F '/' '{print $NF}'";
        logger.info("cmd command: {}", cmd);

        List<String> versions = new ArrayList<>();
        int retVal = execCmd(cmd, versions);
        if (retVal == 0) {
            logger.info("obtain router jar version success");
        }
        return versions;
    }

    public List<String> queryJarPackages(String version) throws Exception {
        String baseDir = StringUtils.joinWith(File.separator, obtainBaseDir(), version);
        Properties props = zkService.getProperties(Constants.COMMON_ROOT + "/" + Constants.GLOBAL_PROPERTIES);
        String user = props.getProperty("user");
        String stormNimbusHost = props.getProperty("storm.nimbus.host");
        String port = props.getProperty("storm.nimbus.port");

        // 0:user name, 1:nimbus host, 2:ssh port
        String cmd = MessageFormat.format("ssh {0}@{1} -p {2} find {3} -type f -a -name \"*.jar\"",
                user, stormNimbusHost, port, baseDir);
        logger.info("cmd command: {}", cmd);

        List<String> jars = new ArrayList<>();
        int retVal = execCmd(cmd, jars);
        if (retVal == 0) {
            logger.info("obtain router jars success");
        }

        return jars;
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
            producer = new KafkaProducer<>(props);

            // 发送重跑控制信息
            if (!StringUtils.equals(newConfig.getProperty("router.topic.offset"), oldConfig.getProperty("router.topic.offset"))) {
                ControlMessage ctrlMsg = new ControlMessage(System.currentTimeMillis(), ControlType.ROUTER_TOPOLOGY_RERUN.name().toUpperCase(), "dbus-web");
                Map<String ,Object> payload = new HashMap<>();
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
            Map<String ,Object> payload = new HashMap<>();
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

    private String obtainBaseDir() throws Exception {
        Properties props = zkService.getProperties(Constants.COMMON_ROOT + "/" + Constants.GLOBAL_PROPERTIES);
        return props.getProperty(JarManagerService.DBUS_ROUTER_JARS_BASE_PATH);
    }

    private int execCmd(String cmd, List<String> lines) {
        int exitValue = -1;
        try {
            Process process = Runtime.getRuntime().exec(cmd);
            Thread outThread = new Thread(new StreamRunnable(process.getInputStream(), lines));
            // Thread errThread = new Thread(new StreamRunnable(process.getErrorStream(), lines));
            outThread.start();
            // errThread.start();
            exitValue = process.waitFor();
            if (lines != null)
                logger.info("result: " + JSON.toJSONString(lines));
            if (exitValue != 0) process.destroyForcibly();
        } catch (Exception e) {
            logger.error("execCmd error", e);
            exitValue = -1;
        }
        return exitValue;
    }

    class StreamRunnable implements Runnable {

        private Reader reader = null;

        private BufferedReader br = null;

        List<String> lines = null;

        public StreamRunnable(InputStream is, List<String> lines) {
            Reader reader = new InputStreamReader(is);
            br = new BufferedReader(reader);
            this.lines = lines;
        }

        @Override
        public void run() {
            try {
                String line = br.readLine();
                while (org.apache.commons.lang.StringUtils.isNotBlank(line)) {
                    logger.info(line);
                    if (lines != null)
                        lines.add(line);
                    line = br.readLine();
                }
            } catch (Exception e) {
                logger.error("stream runnable error", e);
            } finally {
                close(br);
                close(reader);
            }
        }

        private void close(Closeable closeable) {
            try {
                if (closeable != null)
                    closeable.close();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

}
