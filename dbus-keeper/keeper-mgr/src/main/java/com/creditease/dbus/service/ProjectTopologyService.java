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
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.base.com.creditease.dbus.utils.RequestSender;
import com.creditease.dbus.bean.Topology;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.constant.ServiceNames;
import com.creditease.dbus.domain.model.Project;
import com.creditease.dbus.domain.model.ProjectTopology;
import com.creditease.dbus.utils.StormToplogyOpHelper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import javax.websocket.Session;
import java.io.*;
import java.text.MessageFormat;
import java.util.*;

import static com.creditease.dbus.commons.Constants.ROUTER;


/**
 * Created by mal on 2018/4/12.
 */
@Service
public class ProjectTopologyService {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private RequestSender sender;
    @Autowired
    private StormToplogyOpHelper stormTopoHelper;
    @Autowired
    private IZkService zkService;
    @Autowired
    private GrafanaDashBoardService dashBoardService;

    public ResultEntity queryJarVersions() {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/project-topos/versions");
        return result.getBody();
    }

    public ResultEntity queryJarPackages(String version) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/project-topos/packages", "version=" + version);
        return result.getBody();
    }

    public ResultEntity existTopoName(String topoName) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/project-topos/exist/{0}", "", topoName);
        return result.getBody();
    }

    public ResultEntity select(Integer topoId) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/project-topos/select/{0}", "", topoId);
        return result.getBody();
    }

    public ResultEntity delete(Integer topoId) throws Exception {
        ProjectTopology pt = this.select(topoId).getPayload(new TypeReference<ProjectTopology>() {
        });
        if (pt == null)
            return new ResultEntity(MessageCode.TOPOLOGY_NOT_EXIST, "");

        String status = correctStatus(pt.getStatus(), pt.getTopoName());
        if (StringUtils.equals(status, "running") ||
                StringUtils.equals(status, "changed"))
            return new ResultEntity(MessageCode.TOPOLOGY_RUNNING_DO_NOT_DELETE, "");

        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/project-topos/delete/{0}", "", topoId);
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success()) {
            return result.getBody();
        }

        result = sender.get(ServiceNames.KEEPER_SERVICE, "/projects/select/{0}", "", pt.getProjectId());
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
            return result.getBody();
        Project project = result.getBody().getPayload(new TypeReference<Project>() {
        });

        // 删除grafana dashboard router type regex
        dashBoardService.deleteDashboardTemplate(null, null, null,
                project.getProjectName(), pt.getTopoName());
        return result.getBody();
    }

    public ResultEntity update(ProjectTopology record, boolean isStartOrStop) throws Exception {
        if (!isStartOrStop) {
            ProjectTopology pt = select(record.getId()).getPayload(new TypeReference<ProjectTopology>() {
            });
            String status = correctStatus(pt.getStatus(), pt.getTopoName());
            if (StringUtils.equals(status, "running") || StringUtils.equals(status, "changed")) {
            } else {
                record.setStatus(status);
                ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projects/select/{0}", "", pt.getProjectId());
                if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
                    return result.getBody();
                Project project = result.getBody().getPayload(new TypeReference<Project>() {
                });
                saveTopologyConfig(project.getProjectName(), pt.getTopoName(), record.getTopoConfig());
            }
        }
        ResponseEntity<ResultEntity> result = sender.post(ServiceNames.KEEPER_SERVICE, "/project-topos/update", record);
        return result.getBody();
    }

    public ResultEntity insert(ProjectTopology record) throws Exception {
        ResultEntity result = queryTopos(record.getProjectId(), null, 1, 10, null, null);
        if (!(result.getPayload() instanceof Map))
            return result;
        Map<String, Object> payLoad = result.getPayload(new TypeReference<Map<String, Object>>() {
        });
        if ((Integer) payLoad.get("isCanCreateTopology") != 1) {
            result.setStatus(MessageCode.ACHIEVE_TOPOLOGY_MAX_COUNT);
            result.setPayload(null);
            return result;
        }

        result = existTopoName(record.getTopoName());
        Integer existCount = result.getPayload(new TypeReference<Integer>() {
        });
        if (existCount > 0) {
            result.setStatus(MessageCode.TOPOLOGY_NAME_EXIST);
            result.setPayload(null);
            return result;
        }

        record.setUpdateTime(new Date());
        result = sender.post(ServiceNames.KEEPER_SERVICE, "/project-topos/insert", record).getBody();
        return result;
    }

    public ResultEntity queryOutPutTopics(Integer projectId, Integer topoId) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/project-topos/out-topics/{0}/{1}", StringUtils.EMPTY, projectId, topoId);
        return result.getBody();
    }

    public ResultEntity queryInPutTopics(Integer projectId, Integer topoId) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/project-topos/in-topics/{0}/{1}", StringUtils.EMPTY, projectId, topoId);
        return result.getBody();
    }

    public ResultEntity obtainRouterTopologyConfigTemplate() {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/project-topos/template");
        return result.getBody();
    }

    public ResultEntity rerunInit(Integer projectId, Integer topoId) throws Exception {
        ProjectTopology pt = this.select(topoId).getPayload(new TypeReference<ProjectTopology>() {
        });
        if (pt == null) {
            return new ResultEntity(MessageCode.TOPOLOGY_NOT_EXIST, "");
        }

        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/project-topos/rerun-init/{0}/{1}", StringUtils.EMPTY, projectId, topoId);
        List<Map<String, Object>> list = result.getBody().getPayload(new TypeReference<List<Map<String, Object>>>() {
        });
        if (list != null) {
            for (Map<String, Object> item : list) {
                String status = correctStatus(pt.getStatus(), pt.getTopoName());
                if (StringUtils.contains(status, "running") || StringUtils.contains(status, "changed")) {
                    item.put("isCanRerun", true);
                } else {
                    item.put("isCanRerun", false);
                }
            }
            ResultEntity resultEntity = new ResultEntity();
            resultEntity.setPayload(list);
            return resultEntity;
        }
        return result.getBody();
    }

    public ResultEntity rerun(Map<String, String> map) {
        ResponseEntity<ResultEntity> result = sender.post(ServiceNames.KEEPER_SERVICE, "/project-topos/rerun", map);
        return result.getBody();
    }

    public ResultEntity effect(Integer topologyId) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/project-topos/effect/{0}", StringUtils.EMPTY, topologyId);
        return result.getBody();
    }

    /**
     * @param originStatus 管理库的topo状态
     * @param stormStatus  storm的topo状态
     * @return
     */
    public String getTopoStatus(String originStatus, String stormStatus) {
        String status = null;
        if (StringUtils.equals(stormStatus, "stopped")) {
            if (StringUtils.equals(originStatus, "new")) {
                status = originStatus;
            } else {
                status = stormStatus;
            }
        } else if (StringUtils.equals(stormStatus, "running")) {
            if (StringUtils.equals(originStatus, "changed")) {
                status = originStatus;
            } else {
                status = stormStatus;
            }
        }
        return status;
    }

    public String correctStatus(List<Topology> topologies, String originStatus, String topologyName) {
        String stormStatus = "stopped";
        for (Topology topology : topologies) {
            if (StringUtils.equals(topology.getName(), topologyName + "-router")
                    && StringUtils.equals(topology.getStatus(), "ACTIVE")) {
                stormStatus = "running";
            }
        }
        return getTopoStatus(originStatus, stormStatus);
    }

    public String correctStatus(String originStatus, String topologyName) throws Exception {
        List<Topology> topologies = stormTopoHelper.getAllTopologiesInfo();
        return correctStatus(topologies, originStatus, topologyName);
    }

    public ResultEntity queryTopos(Integer projectId,
                                   String topoName,
                                   Integer pageNum,
                                   Integer pageSize,
                                   String sortby,
                                   String order) throws Exception {

        StringBuilder path = new StringBuilder("/project-topos/topos?pageNum=" + pageNum + "&pageSize=" + pageSize);
        if (StringUtils.isNotBlank(sortby))
            path.append("&sortby=" + sortby);

        if (StringUtils.isNotBlank(order))
            path.append("&order=" + order);

        if (projectId != null)
            path.append("&projectId=" + projectId);

        if (StringUtils.isNotBlank(topoName))
            path.append("&topoName=" + topoName);

        Map<String, Object> payLoad = new HashMap<>();
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, path.toString());
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
            return result.getBody();
        Map<String, Object> topos = result.getBody().getPayload(new TypeReference<LinkedHashMap<String, Object>>() {
        });
        if (topos != null) {
            List<Topology> topologies = stormTopoHelper.getAllTopologiesInfo();
            List<Map<String, Object>> datas = (List<Map<String, Object>>) topos.get("list");
            for (Map<String, Object> data : datas) {
                String status = correctStatus(topologies, (String) data.get("status"), (String) data.get("topoName"));
                data.put("status", status);
            }
        }
        payLoad.put("topos", topos);

        if (projectId != null) {
            result = sender.get(ServiceNames.KEEPER_SERVICE, "/projects/select/{0}", StringUtils.EMPTY, projectId);
            if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
                return result.getBody();
            Project project = result.getBody().getPayload(new TypeReference<Project>() {
            });
            if (project == null) {
                result.getBody().setStatus(MessageCode.PROJECT_NOT_EXIST);
                result.getBody().setPayload(null);
                return result.getBody();
            } else {
                payLoad.put("isCanCreateTopology", 0);
                if ((Integer) topos.get("total") < project.getTopologyNum())
                    payLoad.put("isCanCreateTopology", 1);
            }
        }

        result.getBody().setPayload(payLoad);
        return result.getBody();
    }

    public ResultEntity operate(String msg,
                                Session session,
                                Map<String, Object> map,
                                SimpMessagingTemplate smt) throws Exception {
        // {"cmdType":"start/stop", "projectName":"", "topoName":"testdb", "jarPath":""}
        // {"cmdType":"stop", "topoName":"testdb", "id":10, "uid": topoName+time}
        // {"cmdType":"start", "topoName":"testdb", "id":10, "uid": topoName+time}
        Map<String, Object> param = null;
        if (session != null) {
            ObjectMapper mapper = new ObjectMapper();
            param = mapper.readValue(msg, new TypeReference<HashMap<String, Object>>() {
            });
        } else {
            param = map;
        }

        ResultEntity result = null;
        if (StringUtils.equals("start", (String) param.get("cmdType")))
            result = start(param, session, smt);
        else if (StringUtils.equals("stop", (String) param.get("cmdType")))
            result = stop(param, session, smt);
        return result;
    }

    private ResultEntity start(Map<String, Object> param, Session session, SimpMessagingTemplate smt) throws Exception {
        if (session != null) session.getBasicRemote().sendText("请稍等,启动中...");
        if (smt != null) smt.convertAndSendToUser((String) param.get("uid"), "/log", "请稍等,启动中...");
        String topologyName = (String) param.get("topoName");
        String projectName = (String) param.get("projectName");
        String jarFilePath = (String) param.get("jarPath");
        String alias = (String) param.get("alias");

        Properties props = zkService.getProperties(Constants.COMMON_ROOT + "/" + Constants.GLOBAL_PROPERTIES);
        String stormHomePath = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOME_PATH);
        if (StringUtils.endsWith(stormHomePath, "/"))
            stormHomePath = StringUtils.substringBeforeLast(stormHomePath, "/");

        String routerJarsBasePath = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_JARS_PATH);
        if (StringUtils.endsWith(routerJarsBasePath, "/")) {
            routerJarsBasePath = StringUtils.substringBeforeLast(routerJarsBasePath, "/");
        }

        String zkUrl = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_ZK_STR);
        String user = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_USER);
        String stormNimbusHost = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOST);
        String port = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_PORT);

        // 0:routerJarsBasePath, 1:stormHomePath, 2:zk url, 3: topologyName, 4:jar path
        // 5:user name, 6:nimbus host, 7:ssh port, 8:project name, 9: alias
        String cmd = MessageFormat.format("ssh -p {7} {5}@{6} {0}/dbus_startTopology.sh {1} router {2} {3} {4} {8} {9}",
                routerJarsBasePath, stormHomePath, zkUrl, topologyName, jarFilePath, user, stormNimbusHost, port, projectName, alias);
        logger.info("start topology command: {}", cmd);

        Map<String, Object> retMap = execCmd(cmd, session, true, smt, (String) param.get("uid"));
        logger.info("start topology return code: {}, param: {}", retMap.get("code"), JSON.toJSONString(param));
        if ((Integer) retMap.get("code") == 0) {
            ProjectTopology record = new ProjectTopology();
            record.setId((Integer) param.get("id"));
            record.setStatus("running");
            update(record, true);
        }
        return new ResultEntity(ResultEntity.SUCCESS, (String) retMap.get("msg"));
    }

    private ResultEntity stop(Map<String, Object> param, Session session, SimpMessagingTemplate smt) throws Exception {
        if (session != null) session.getBasicRemote().sendText("请稍等,停止中...");
        if (smt != null) smt.convertAndSendToUser((String) param.get("uid"), "/log", "请稍等,停止中...");
        String topologyName = (String) param.get("topoName");

        Properties props = zkService.getProperties(Constants.COMMON_ROOT + "/" + Constants.GLOBAL_PROPERTIES);
        String user = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_USER);
        String stormNimbusHost = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOST);
        String stormHomePath = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOME_PATH);
        if (StringUtils.endsWith(stormHomePath, "/"))
            stormHomePath = StringUtils.substringBeforeLast(stormHomePath, "/");

        String port = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_PORT);

        String cmd = MessageFormat.format("ssh -p {4} {2}@{3} {0}/bin/storm kill {1}-router -w 10",
                stormHomePath, topologyName, user, stormNimbusHost, port);
        logger.info("stop topology command: {}", cmd);

        Map<String, Object> retMap = execCmd(cmd, session, false, smt, (String) param.get("uid"));
        logger.info("stop topology return code: {}, param: {}", retMap.get("code"), JSON.toJSONString(param));
        if ((Integer) retMap.get("code") == 0) {
            ProjectTopology record = new ProjectTopology();
            record.setId((Integer) param.get("id"));
            record.setStatus("stopped");
            update(record, true);
        }
        return new ResultEntity(ResultEntity.SUCCESS, (String) retMap.get("msg"));
    }

    private Map<String, Object> execCmd(String cmd, Session session, boolean isStart, SimpMessagingTemplate smt, String uid) {
        Map<String, Object> retMap = new HashMap<>();
        int exitValue = -1;
        StringBuilder out = new StringBuilder();
        StringBuilder error = new StringBuilder();
        try {
            Process process = Runtime.getRuntime().exec(cmd);
            Thread outThread = new Thread(new StreamRunnable(process.getInputStream(), session, out, smt, uid));
            Thread errThread = new Thread(new StreamRunnable(process.getErrorStream(), session, error, smt, uid));
            outThread.start();
            errThread.start();
            exitValue = process.waitFor();
            if (exitValue != 0) process.destroyForcibly();
            logger.info("start or stop router topology out info:{}", out.toString());
            logger.info("start or stop router topology error info:{}", error.toString());
            if (isStart) {
                if (StringUtils.contains(out.toString(), "Finished submitting topology") ||
                        StringUtils.contains(out.toString(), "already exists on cluster") ||
                        StringUtils.contains(error.toString(), "already exists on cluster")) exitValue = 0;
                else exitValue = -1;
            } else {
                if (StringUtils.contains(out.toString(), "Killed topology") ||
                        StringUtils.contains(out.toString(), "NotAliveException") ||
                        StringUtils.contains(error.toString(), "NotAliveException")) exitValue = 0;
                else exitValue = -1;
            }
        } catch (Exception e) {
            logger.error("execCmd error", e);
            exitValue = -1;
        }
        retMap.put("code", exitValue);
        if (out.length() != 0) retMap.put("msg", out.toString());
        else retMap.put("msg", error.toString());
        return retMap;
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

    public ResultEntity getTopoAlias(Integer topoId) {
        return sender.get(ServiceNames.KEEPER_SERVICE, "/project-topos/getTopoAlias/{0}", topoId).getBody();
    }

    class StreamRunnable implements Runnable {

        private Reader reader = null;

        private BufferedReader br = null;

        private Session session = null;

        private SimpMessagingTemplate smt = null;

        private StringBuilder sb = null;

        private String uid = null;

        public StreamRunnable(InputStream is, Session session, StringBuilder sb, SimpMessagingTemplate smt, String uid) {
            Reader reader = new InputStreamReader(is);
            br = new BufferedReader(reader);
            this.session = session;
            this.sb = sb;
            this.smt = smt;
            this.uid = uid;
        }

        @Override
        public void run() {
            try {
                String line = br.readLine();
                while (StringUtils.isNotBlank(line)) {
                    if (session != null)
                        session.getBasicRemote().sendText(line);
                    if (smt != null)
                        smt.convertAndSendToUser(uid, "/log", line);
                    sb.append(line);
                    sb.append(SystemUtils.LINE_SEPARATOR);
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
