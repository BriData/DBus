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


package com.creditease.dbus.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.bean.Topology;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.domain.model.DataSource;
import com.creditease.dbus.domain.model.StormTopology;
import com.creditease.dbus.enums.DbusDatasourceType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.security.kerberos.client.KerberosRestTemplate;
import org.springframework.web.client.RestTemplate;

import java.text.MessageFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by Hongchunyin on 2018/3/19.
 */
public class StormToplogyOpHelper {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public String TOPO_AVAILABLE_ALL_RUNNING = "ALL_RUNNING";
    public String TOPO_AVAILABLE_ALL_STOPPED = "ALL_STOPPED";
    public String TOPO_AVAILABLE_PART_RUNNING = "PART_RUNNING";

    private static RestTemplate restTemplate;
    public String stormRestApi = "";
    private IZkService zk;

    public StormToplogyOpHelper(IZkService zk) throws Exception {
        this.zk = zk;
        if (zk.isExists(KeeperConstants.GLOBAL_CONF)) {
            Properties properties = zk.getProperties(Constants.GLOBAL_PROPERTIES_ROOT);
            stormRestApi = properties.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_REST_URL);
            if (StringUtils.equals(SecurityConfProvider.getSecurityConf(zk), Constants.SECURITY_CONFIG_TRUE_VALUE)) {
                restTemplate = new KerberosRestTemplate(properties.getProperty(KeeperConstants.GLOBAL_CONF_KEY_KEYTAB_PATH),
                        properties.getProperty(KeeperConstants.GLOBAL_CONF_KEY_PRINCIPAL));
            } else {
                restTemplate = new RestTemplate();
            }
        } else {
            restTemplate = new RestTemplate();
        }
    }

    public Map<String, StormTopology> getRunningTopologies() throws Exception {
        Map<String, StormTopology> runningTopologies = new HashMap<>();
        List<Topology> topologies = topologySummary();
        topologies.forEach(topology -> {
            String topoName = topology.getName();
            StormTopology topo = new StormTopology(topoName);
            topo.setTopologyId(topology.getId());
            topo.setUptime(topology.getUptime());
            runningTopologies.put(topoName, topo);
        });
        return runningTopologies;
    }

    public List<Topology> getAllTopologiesInfo() throws Exception {
        return topologySummary();
    }

    public void populateToposToDs(List<Map<String, Object>> dataSources) throws Exception {
        Map runningTopologies = getRunningTopologies();
        populateToposToEachDs(runningTopologies, dataSources);
    }

    public Topology getTopologyByName(String name) throws Exception {
        List<Topology> collect = topologySummary().stream().filter(topology -> topology.getName().equals(name)).collect(Collectors.toList());
        return collect != null && !collect.isEmpty() ? collect.get(0) : null;
    }

    public String stopTopology(String topologyId, Integer waitTime, String priKeyPath) throws Exception {
        if (waitTime == null) {
            waitTime = 10;
        }
        int index = topologyId.lastIndexOf("-");
        int index1 = topologyId.lastIndexOf("-", index - 1);
        String topologyName = topologyId.substring(0, index1);
        Properties props = zk.getProperties(Constants.COMMON_ROOT + "/" + Constants.GLOBAL_PROPERTIES);
        String user = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_USER);
        String stormNimbusHost = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOST);
        String stormHomePath = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOME_PATH);
        if (StringUtils.endsWith(stormHomePath, "/")) {
            stormHomePath = StringUtils.substringBeforeLast(stormHomePath, "/");
        }
        String port = props.getProperty(KeeperConstants.GLOBAL_CONF_KEY_CLUSTER_SERVER_SSH_PORT);

        String cmd = MessageFormat.format("{0}/bin/storm kill {1} -w {2}", stormHomePath, topologyName, waitTime);
        logger.info("stop topology command: {}", cmd);

        String errMessage = SSHUtils.executeCommand(user, stormNimbusHost, Integer.parseInt(port), priKeyPath, cmd, true);
        if (StringUtils.isNotBlank(errMessage)) {
            return "error";
        }
        return "ok";
    }


    private void populateToposToEachDs(Map runningTopologies, List<Map<String, Object>> dataSources) {
        for (Map<String, Object> dataSource : dataSources) {
            String dsName = (String) dataSource.get(DataSource.KEY_NAME);
            String type = (String) dataSource.get(DataSource.KEY_TYPE);
            Map toposOfDsMap = new HashMap<>();

            String allTopoForDsAvailable = TOPO_AVAILABLE_PART_RUNNING;

            //if (DbusDatasourceType.MYSQL.name().equalsIgnoreCase(type)) {
            //    String mysqlExtractorTopoAvailable = TOPO_AVAILABLE_ALL_RUNNING;
            //
            //    String extractorTopoName = dsName + "-mysql-extractor";
            //    if (runningTopologies.containsKey(extractorTopoName)) {
            //        toposOfDsMap.put(extractorTopoName, runningTopologies.get(extractorTopoName));
            //        mysqlExtractorTopoAvailable = TOPO_AVAILABLE_ALL_RUNNING;
            //    } else {
            //        mysqlExtractorTopoAvailable = TOPO_AVAILABLE_ALL_STOPPED;
            //        toposOfDsMap.put(extractorTopoName, new StormTopology(extractorTopoName));
            //    }
            //    String dispatcherAppenderTopoName = dsName + "-dispatcher-appender";
            //    String[] streamSeperatedToposName = {dsName + "-dispatcher", dsName + "-appender"};
            //    String streamTopoForDsAvailable = combinedTopoProcessing(runningTopologies, toposOfDsMap, dispatcherAppenderTopoName, streamSeperatedToposName);
            //
            //    String splitterPullerTopoName = dsName + "-splitter-puller";
            //    String[] fullSeperatedToposName = {dsName + "-splitter", dsName + "-puller"};
            //    String fullTopoForDsAvailable = combinedTopoProcessing(runningTopologies, toposOfDsMap, splitterPullerTopoName, fullSeperatedToposName);
            //
            //    if (mysqlExtractorTopoAvailable.equals(TOPO_AVAILABLE_ALL_RUNNING)
            //            && streamTopoForDsAvailable.equals(TOPO_AVAILABLE_ALL_RUNNING)
            //            && fullTopoForDsAvailable.equals(TOPO_AVAILABLE_ALL_RUNNING)) {
            //        allTopoForDsAvailable = TOPO_AVAILABLE_ALL_RUNNING;
            //    } else if (mysqlExtractorTopoAvailable.equals(TOPO_AVAILABLE_ALL_STOPPED)
            //            && streamTopoForDsAvailable.equals(TOPO_AVAILABLE_ALL_STOPPED)
            //            && fullTopoForDsAvailable.equals(TOPO_AVAILABLE_ALL_STOPPED)) {
            //        allTopoForDsAvailable = TOPO_AVAILABLE_ALL_STOPPED;
            //    }
            //}

            if (DbusDatasourceType.ORACLE.name().equalsIgnoreCase(type) || DbusDatasourceType.MONGO.name().equalsIgnoreCase(type)
                    || DbusDatasourceType.DB2.name().equalsIgnoreCase(type) || DbusDatasourceType.MYSQL.name().equalsIgnoreCase(type)) {
                String dispatcherAppenderTopoName = dsName + "-dispatcher-appender";
                String[] streamSeperatedToposName = {dsName + "-dispatcher", dsName + "-appender"};
                String streamTopoForDsAvailable = combinedTopoProcessing(runningTopologies, toposOfDsMap, dispatcherAppenderTopoName, streamSeperatedToposName);

                String splitterPullerTopoName = dsName + "-splitter-puller";
                String[] fullSeperatedToposName = {dsName + "-splitter", dsName + "-puller"};
                String fullTopoForDsAvailable = combinedTopoProcessing(runningTopologies, toposOfDsMap, splitterPullerTopoName, fullSeperatedToposName);
                if (streamTopoForDsAvailable.equals(TOPO_AVAILABLE_ALL_RUNNING) && fullTopoForDsAvailable.equals(TOPO_AVAILABLE_ALL_RUNNING)) {
                    allTopoForDsAvailable = TOPO_AVAILABLE_ALL_RUNNING;
                } else if (streamTopoForDsAvailable.equals(TOPO_AVAILABLE_ALL_STOPPED) && fullTopoForDsAvailable.equals(TOPO_AVAILABLE_ALL_STOPPED)) {
                    allTopoForDsAvailable = TOPO_AVAILABLE_ALL_STOPPED;
                }
            }

            if (type.toLowerCase().indexOf("log") != -1) {
                String logTopoName = dsName + "-log-processor";

                if (runningTopologies.containsKey(logTopoName)) {
                    toposOfDsMap.put(logTopoName, runningTopologies.get(logTopoName));
                    allTopoForDsAvailable = TOPO_AVAILABLE_ALL_RUNNING;
                } else {
                    toposOfDsMap.put(logTopoName, new StormTopology(logTopoName));
                    allTopoForDsAvailable = TOPO_AVAILABLE_ALL_STOPPED;
                }
            }

            dataSource.put("topoAvailableStatus", allTopoForDsAvailable);
            dataSource.put("toposOfDs", toposOfDsMap);
        }
    }

    private String combinedTopoProcessing(Map runningTopologies, Map toposOfDsMap, String combinedTopoName, String[] seperatedToposName) {
        String checkResult = TOPO_AVAILABLE_ALL_RUNNING;
        boolean topoAvailableFlag = true;
        boolean existedLivingSperatedTopo = false;
        if (runningTopologies.containsKey(combinedTopoName)) {
            toposOfDsMap.put(combinedTopoName, runningTopologies.get(combinedTopoName));
        } else {
            for (String seperatedTopo : seperatedToposName) {
                if (runningTopologies.containsKey(seperatedTopo)) {
                    existedLivingSperatedTopo = true;
                    break;
                }
            }

            topoAvailableFlag = false;
            if (existedLivingSperatedTopo) {
                int topoCounter = 0;
                for (String seperatedTopo : seperatedToposName) {
                    if (runningTopologies.containsKey(seperatedTopo)) {
                        toposOfDsMap.put(seperatedTopo, runningTopologies.get(seperatedTopo));
                        topoCounter++;
                    } else {
                        toposOfDsMap.put(seperatedTopo, new StormTopology(seperatedTopo));
                    }
                }
                int expetedTopoCount = seperatedToposName.length;
                if (topoCounter == expetedTopoCount) {
                    topoAvailableFlag = true;
                    checkResult = TOPO_AVAILABLE_ALL_RUNNING;
                } else {
                    checkResult = TOPO_AVAILABLE_PART_RUNNING;
                }
            }
        }

        if (!topoAvailableFlag && !existedLivingSperatedTopo) {
            toposOfDsMap.put(combinedTopoName, new StormTopology(combinedTopoName));
            checkResult = TOPO_AVAILABLE_ALL_STOPPED;
        }

        return checkResult;
    }

    //storm worker数达到300+ nimbus有时候会莫名卡住,该方法一直无返回值
    //public String topologySummary() throws Exception {
    //    return getForResult(stormRestApi + "/topology/summary");
    //}

    public List<Topology> topologySummary() throws Exception {
        //storm.zookeeper.root
        Properties properties = zk.getProperties(Constants.GLOBAL_PROPERTIES_ROOT);
        String stormRoot = properties.getProperty("storm.zookeeper.root");
        if (!stormRoot.startsWith("/")) {
            stormRoot = "/" + stormRoot;
        }
        List<String> children = zk.getChildren(stormRoot + "/storms");
        List<Topology> topologies = new ArrayList<>();
        children.forEach(s -> {
            Topology topology = new Topology();
            int index = s.lastIndexOf("-");
            int index1 = s.lastIndexOf("-", index - 1);
            topology.setName(s.substring(0, index1));
            topology.setId(s);
            topology.setStatus("ACTIVE");
            topology.setUptime(diffDate(System.currentTimeMillis() - Long.parseLong(s.substring(index + 1)) * 1000));
            topologies.add(topology);
        });
        return topologies;
    }

    public String getTopoRunningInfoById(String topologyId) {
        String topoWorkers = getForResult(stormRestApi + "/topology-workers/" + topologyId);
        //目前都是单worker部署,取index 0
        JSONObject topoWorkersObj = JSON.parseObject(topoWorkers);
        JSONArray hostPortInfo = topoWorkersObj.getJSONArray("hostPortList");
        if (!hostPortInfo.isEmpty()) {
            String hostInfo = hostPortInfo.getJSONObject(0).getString("host");
            String port = hostPortInfo.getJSONObject(0).getString("port");
            return String.format("%s:%s", hostInfo, port);
        }
        return "";
//        String url = stormRestApi + "/topology/" + topologyId + "/visualization";
//        logger.info("url:{}", url);
//        String result = getForResult(url);
//        JSONObject topoInfo = JSON.parseObject(result);
//        String host = null;
//        String port = null;
//        for (Map.Entry<String, Object> entry : topoInfo.entrySet()) {
//            JSONObject value = (JSONObject) entry.getValue();
//            if (value.containsKey(":stats")) {
//                JSONObject stats = value.getJSONArray(":stats").getJSONObject(0);
//                host = stats.getString(":host");
//                port = stats.getString(":port");
//                break;
//            }
//        }
//        return String.format("%s:%s", host, port);
    }

    public String nimbusSummary() {
        return getForResult(stormRestApi + "/nimbus/summary");
    }

    public String supervisorSummary() {
        return getForResult(stormRestApi + "/supervisor/summary");
    }

    private String getForResult(String api) {
        String result = null;
        ResponseEntity<String> responseEntity = restTemplate.getForEntity(api, String.class);
        if (responseEntity.getStatusCode().is2xxSuccessful()) {
            result = responseEntity.getBody();
        }
        return result;
    }

    /**
     * diff 毫秒
     *
     * @param diff
     * @return
     */
    public String diffDate(long diff) {
        long nd = 1000 * 24 * 60 * 60;
        long nh = 1000 * 60 * 60;
        long nm = 1000 * 60;
        long ns = 1000;
        // 计算差多少天
        long day = diff / nd;
        // 计算差多少小时
        long hour = diff % nd / nh;
        // 计算差多少分钟
        long min = diff % nd % nh / nm;
        // 计算差多少秒//输出结果
        long sec = diff % nd % nh % nm / ns;

        StringBuilder ret = new StringBuilder();
        if (day != 0) {
            ret.append(day + "d ");
        }
        if (hour != 0) {
            ret.append(hour + "h ");
        }
        if (min != 0) {
            ret.append(min + "m ");
        }
        if (sec != 0) {
            ret.append(sec + "s");
        }
        return ret.length() == 0 ? "s" : ret.toString();
    }

}
