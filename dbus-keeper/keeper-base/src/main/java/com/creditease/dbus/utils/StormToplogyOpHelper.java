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

package com.creditease.dbus.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.*;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.domain.model.DataSource;
import com.creditease.dbus.domain.model.StormTopology;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.jcraft.jsch.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Hongchunyin on 2018/3/19.
 */
public class StormToplogyOpHelper {
    private static Logger logger = LoggerFactory.getLogger(StormToplogyOpHelper.class);

    public static String TOPO_AVAILABLE_ALL_RUNNING = "ALL_RUNNING";
    public static String TOPO_AVAILABLE_ALL_STOPPED = "ALL_STOPPED";
    public static String TOPO_AVAILABLE_PART_RUNNING = "PART_RUNNING";

    public static String OP_RESULT_SUCCESS = "success";

    public static boolean inited = false;
    public static String stormRestApi = "";
    public static String loginUrl = "";
    public static String user = "";
    public static String pass = "";


    public static void init(IZkService zkService) throws Exception {
            stormRestApi = (String) zkService.getProperties(KeeperConstants.GLOBAL_CONF).get(KeeperConstants.GLOBAL_CONF_KEY_STORM_REST_API);
            inited = true;
    }

    public static Map getRunningTopologies(List<Map<String, Object>> dataSources) {
        Map runningTopologies = new HashMap<>();
        JSONObject topologySummary = JSON.parseObject(topologySummary());
        JSONArray toposArr = topologySummary.getJSONArray("topologies");
        for (int i = 0; i < toposArr.size(); i++) {
            JSONObject jsonObject = toposArr.getJSONObject(i);
            String topoName = jsonObject.getString("name");
            StormTopology topo = new StormTopology(topoName);
            topo.setTopologyId(jsonObject.getString("id"));
            topo.setUptime(jsonObject.getString("uptime"));
            runningTopologies.put(topoName, topo);
        }
        return runningTopologies;
    }

    public static String getTopoRunningInfoById(String topologyId) {
        String topoWorkers = getForResult(stormRestApi + "/topology-workers/" + topologyId);
        JSONObject topoWorkersObj = JSON.parseObject(topoWorkers);

        JSONArray hostPortInfo = topoWorkersObj.getJSONArray("hostPortList");
        if (!hostPortInfo.isEmpty()) {
            String hostInfo = hostPortInfo.getJSONObject(0).getString("host");
            String port = hostPortInfo.getJSONObject(0).getString("port");
            String runningInfo = hostInfo + ":" + topologyId + "/" + port;
            return runningInfo;
        }
        return null;
    }

    public static void populateToposToDs(List<Map<String, Object>> dataSources) {
        Map runningTopologies = getRunningTopologies(dataSources);
        populateToposToEachDs(runningTopologies, dataSources);

    }

    public static String killTopology(String topologyId, int waitTime) {
        String topologyKillApi = stormRestApi + "/topology/" + topologyId + "/kill/" + waitTime;
        JSONObject resultJson = null;
            RestTemplate restTemplate = new RestTemplate();
            ResponseEntity<JSONObject> result = restTemplate.postForEntity(topologyKillApi, new HttpEntity<>("", new HttpHeaders()), JSONObject.class);
            resultJson = result.getBody();
        return resultJson.getString("status");
    }

    public static String execute(final String pubKeyPath, final String username, final String host, final int port, final String command) throws JSchException {
        StringBuffer opResult = new StringBuffer();
        StringBuffer errResult = new StringBuffer();

        JSch jsch = new JSch();
        jsch.addIdentity(pubKeyPath);
        try {
            // Create and connect session.
            Session session = jsch.getSession(username, host, port);
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();

            // Create and connect channel.
            Channel channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(command);

            channel.setInputStream(null);
            /*BufferedReader input = new BufferedReader(new InputStreamReader(channel
                    .getInputStream()));*/
            InputStream in = channel.getInputStream();
            InputStream err = channel.getExtInputStream();
            channel.connect();
            /*
            // Get the output of remote command.
            String line;
            while ((line = input.readLine()) != null) {
                opResult.append(line);
            }*/

            //一直等命令执行完毕，然后获取结果
            byte[] tmp = new byte[1024];
            while (true) {
                while (in.available() > 0) {
                    int i = in.read(tmp, 0, 1024);
                    if (i < 0) break;
                    opResult.append(new String(tmp, 0, i));
                }
                while (err.available() > 0) {
                    int i = err.read(tmp, 0, 1024);
                    if (i < 0) break;
                    errResult.append(new String(tmp, 0, i));
                }
                if (channel.isClosed()) {
                    if ((in.available() > 0) || (err.available() > 0)) continue;
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                }
            }

            //input.close();

            // Disconnect the channel and session.
            channel.disconnect();
            session.disconnect();
        } catch (JSchException e) {
            logger.error("Execute ssh command {} failed.{}", command, e);
        } catch (Exception e) {
            logger.error("Execute ssh command {} failed.{}", command, e);
        }
        return errResult.length() > 0 ? errResult.toString() : opResult.toString();
    }

    private static void populateToposToEachDs(Map runningTopologies, List<Map<String, Object>> dataSources) {
        for (Map<String, Object> dataSource : dataSources) {
            String dsName = (String) dataSource.get(DataSource.KEY_NAME);
            String type = (String) dataSource.get(DataSource.KEY_TYPE);
            Map toposOfDsMap = new HashMap<>();

            String allTopoForDsAvailable = TOPO_AVAILABLE_PART_RUNNING;

            if (DbusDatasourceType.MYSQL.name().equalsIgnoreCase(type)) {
                String mysqlExtractorTopoAvailable = TOPO_AVAILABLE_ALL_RUNNING;

                String extractorTopoName = dsName + "-mysql-extractor";
                if (runningTopologies.containsKey(extractorTopoName)) {
                    toposOfDsMap.put(extractorTopoName, runningTopologies.get(extractorTopoName));
                    mysqlExtractorTopoAvailable = TOPO_AVAILABLE_ALL_RUNNING;
                } else {
                    mysqlExtractorTopoAvailable = TOPO_AVAILABLE_ALL_STOPPED;
                    toposOfDsMap.put(extractorTopoName, new StormTopology(extractorTopoName));
                }
                String dispatcherAppenderTopoName = dsName + "-dispatcher-appender";
                String[] streamSeperatedToposName = {dsName + "-dispatcher", dsName + "-appender"};
                String streamTopoForDsAvailable = combinedTopoProcessing(runningTopologies, toposOfDsMap, dispatcherAppenderTopoName, streamSeperatedToposName);

                String splitterPullerTopoName = dsName + "-splitter-puller";
                String[] fullSeperatedToposName = {dsName + "-splitter", dsName + "-puller"};
                String fullTopoForDsAvailable = combinedTopoProcessing(runningTopologies, toposOfDsMap, splitterPullerTopoName, fullSeperatedToposName);

                if (mysqlExtractorTopoAvailable.equals(TOPO_AVAILABLE_ALL_RUNNING)
                        && streamTopoForDsAvailable.equals(TOPO_AVAILABLE_ALL_RUNNING)
                        && fullTopoForDsAvailable.equals(TOPO_AVAILABLE_ALL_RUNNING)) {
                    allTopoForDsAvailable = TOPO_AVAILABLE_ALL_RUNNING;
                } else if (mysqlExtractorTopoAvailable.equals(TOPO_AVAILABLE_ALL_STOPPED)
                        && streamTopoForDsAvailable.equals(TOPO_AVAILABLE_ALL_STOPPED)
                        && fullTopoForDsAvailable.equals(TOPO_AVAILABLE_ALL_STOPPED)) {
                    allTopoForDsAvailable = TOPO_AVAILABLE_ALL_STOPPED;
                }
            }

            if (DbusDatasourceType.ORACLE.name().equalsIgnoreCase(type)) {
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

    private static String combinedTopoProcessing(Map runningTopologies, Map toposOfDsMap, String combinedTopoName, String[] seperatedToposName) {
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

    public static String topologySummary() {
        return getForResult(stormRestApi + "/topology/summary");
    }

    public static String nimbusSummary() {
        return getForResult(stormRestApi + "/nimbus/summary");
    }

    public static String supervisorSummary() {
        return getForResult(stormRestApi + "/supervisor/summary");
    }

    private static String getForResult(String api) {
        String result = null;
            RestTemplate restTemplate = new RestTemplate();
            result = restTemplate.getForObject(api, String.class);
        return result;
    }
}
