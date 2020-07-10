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

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.bean.DeployInfoBean;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.domain.model.DataSource;
import com.creditease.dbus.utils.JsonFormatUtils;
import com.creditease.dbus.utils.SSHUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This is Description
 *
 * @author xiancangao
 * @date 2018/12/11
 */
@Service
public class AutoDeployDataLineService {

    Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private IZkService zkService;
    @Autowired
    private Environment env;
    @Autowired
    private ToolSetService toolSetService;

    private JSONObject getZKNodeData(String path) throws Exception {
        JSONObject json = new JSONObject();
        if (zkService.isExists(path)) {
            byte[] data = zkService.getData(path);
            if (data != null && data.length > 0) {
                json = JSONObject.parseObject(new String(data, KeeperConstants.UTF8));
            }
        }
        return json;
    }

    public JSONObject getOggConf(String dsName) throws Exception {
        JSONObject oggConfJson = getZKNodeData(Constants.OGG_PROPERTIES_ROOT);
        JSONObject dsOggConf = oggConfJson.getJSONObject(KeeperConstants.DS_OGG_CONF);
        JSONObject oggDeployConf = oggConfJson.getJSONObject(KeeperConstants.DEPLOY_CONF);
        if (dsOggConf != null) {
            JSONObject oggConf = dsOggConf.getJSONObject(dsName);
            if (oggConf != null) {
                oggConfJson.put(KeeperConstants.REPLICAT_NAME, oggConf.getString(KeeperConstants.REPLICAT_NAME));
                oggConfJson.put(KeeperConstants.TRAIL_NAME, oggConf.getString(KeeperConstants.TRAIL_NAME));
                oggConfJson.put(KeeperConstants.HOST, oggConf.getString(KeeperConstants.HOST));
                oggConfJson.put(KeeperConstants.NLS_LANG, oggConf.getString(KeeperConstants.NLS_LANG));
                if (oggDeployConf != null) {
                    JSONObject oggHostInfo = oggDeployConf.getJSONObject(oggConf.getString(KeeperConstants.HOST));
                    if (oggHostInfo != null) {
                        oggConfJson.put(KeeperConstants.OGG_TRAIL_PATH, oggHostInfo.getString(KeeperConstants.OGG_TRAIL_PATH));
                        oggConfJson.put(KeeperConstants.OGG_PATH, oggHostInfo.getString(KeeperConstants.OGG_PATH));
                        oggConfJson.put(KeeperConstants.OGG_TOOL_PATH, oggHostInfo.getString(KeeperConstants.OGG_TOOL_PATH));
                        oggConfJson.put(KeeperConstants.USER, oggHostInfo.getString(KeeperConstants.USER));
                        oggConfJson.put(KeeperConstants.PORT, oggHostInfo.getString(KeeperConstants.PORT));
                        oggConfJson.put(KeeperConstants.MGR_REPLICAT_PORT, oggHostInfo.getString(KeeperConstants.MGR_REPLICAT_PORT));
                    }
                }
            }
        }
        oggConfJson.put("dsName", dsName);
        return oggConfJson;
    }

    public void deleteOggConf(String dsName) throws Exception {
        JSONObject canalConfJson = getZKNodeData(Constants.OGG_PROPERTIES_ROOT);
        JSONObject dsCanalConf = canalConfJson.getJSONObject(KeeperConstants.DS_OGG_CONF);
        if (dsCanalConf != null) {
            dsCanalConf.remove(dsName);
        }
        zkService.setData(Constants.OGG_PROPERTIES_ROOT,
                JsonFormatUtils.toPrettyFormat(canalConfJson.toJSONString()).getBytes(KeeperConstants.UTF8));
    }

    public void setOggConf(Map<String, String> map) throws Exception {
        String dsName = map.get("dsName");

        JSONObject oggConfJson = getZKNodeData(Constants.OGG_PROPERTIES_ROOT);
        String host = map.get(KeeperConstants.HOST).trim();
        String hosts = oggConfJson.getString(KeeperConstants.HOSTS);
        hosts = StringUtils.isBlank(hosts) ? host : hosts;
        if (!hosts.contains(host)) {
            hosts = hosts + "," + host;
        }
        oggConfJson.put(KeeperConstants.HOSTS, hosts);

        JSONObject oggDeployConf = oggConfJson.getJSONObject(KeeperConstants.DEPLOY_CONF);
        oggDeployConf = oggDeployConf != null ? oggDeployConf : new JSONObject();

        JSONObject oggHostInfo = oggDeployConf.getJSONObject(host);
        oggHostInfo = oggHostInfo != null ? oggHostInfo : new JSONObject();

        if (StringUtils.isNotBlank(map.get(KeeperConstants.PORT))) {
            oggHostInfo.put(KeeperConstants.PORT, map.get(KeeperConstants.PORT));
        }
        if (StringUtils.isNotBlank(map.get(KeeperConstants.USER))) {
            oggHostInfo.put(KeeperConstants.USER, map.get(KeeperConstants.USER));
        }
        if (StringUtils.isNotBlank(map.get(KeeperConstants.OGG_PATH))) {
            oggHostInfo.put(KeeperConstants.OGG_PATH, map.get(KeeperConstants.OGG_PATH));
        }
        if (StringUtils.isNotBlank(map.get(KeeperConstants.OGG_TOOL_PATH))) {
            oggHostInfo.put(KeeperConstants.OGG_TOOL_PATH, map.get(KeeperConstants.OGG_TOOL_PATH));
        }
        oggDeployConf.put(host, oggHostInfo);
        oggConfJson.put(KeeperConstants.DEPLOY_CONF, oggDeployConf);

        JSONObject dsOggConf = oggConfJson.getJSONObject(KeeperConstants.DS_OGG_CONF);
        dsOggConf = dsOggConf != null ? dsOggConf : new JSONObject();

        JSONObject oggConf = oggConfJson.getJSONObject(dsName);
        oggConf = oggConf != null ? oggConf : new JSONObject();

        if (StringUtils.isNotBlank(map.get(KeeperConstants.REPLICAT_NAME))) {
            oggConf.put(KeeperConstants.REPLICAT_NAME, map.get(KeeperConstants.REPLICAT_NAME).toLowerCase());
        }
        if (StringUtils.isNotBlank(map.get(KeeperConstants.TRAIL_NAME))) {
            oggConf.put(KeeperConstants.TRAIL_NAME, map.get(KeeperConstants.TRAIL_NAME));
        }
        if (StringUtils.isNotBlank(map.get(KeeperConstants.NLS_LANG))) {
            oggConf.put(KeeperConstants.NLS_LANG, map.get(KeeperConstants.NLS_LANG));
        }
        oggConf.put(KeeperConstants.HOST, host);
        dsOggConf.put(dsName, oggConf);

        oggConfJson.put(KeeperConstants.DS_OGG_CONF, dsOggConf);
        zkService.setData(Constants.OGG_PROPERTIES_ROOT,
                JsonFormatUtils.toPrettyFormat(oggConfJson.toJSONString()).getBytes(KeeperConstants.UTF8));
    }

    public String getOggTrailName(Integer number) throws Exception {
        if (number == null) {
            number = 10;
        }
        StringBuilder sb = new StringBuilder();

        JSONObject oggConfJson = getZKNodeData(Constants.OGG_PROPERTIES_ROOT);
        JSONObject dsOggConf = oggConfJson.getJSONObject(KeeperConstants.DS_OGG_CONF);
        HashSet<String> trailNames = new HashSet<>();
        if (dsOggConf != null) {
            for (Object value : dsOggConf.values()) {
                String trailName = ((JSONObject) value).getString(KeeperConstants.TRAIL_NAME);
                trailNames.add(trailName);
            }
        }
        for (int n = 0; n < number; n++) {
            String trailName = generateOggTrailName(trailNames);
            sb.append(trailName).append(",");
            trailNames.add(trailName);
        }
        return sb.substring(0, sb.length() - 1);
    }

    private String generateOggTrailName(HashSet<String> trailNames) {
        for (int i = 97; i <= 122; i++) {
            for (int j = 97; j <= 122; j++) {
                String newTrailName = "" + (char) i + (char) j;
                if (!trailNames.contains(newTrailName)) {
                    return newTrailName;
                }
            }
        }
        return null;
    }

    public List<Integer> getCanalSlaveId(Integer number) throws Exception {
        JSONObject canalConfJson = getZKNodeData(Constants.CANAL_PROPERTIES_ROOT);
        HashSet<Integer> slavaIds = new HashSet<>();
        JSONObject dsCanalConf = canalConfJson.getJSONObject(KeeperConstants.DS_CANAL_CONF);
        if (dsCanalConf != null) {
            for (Object value : dsCanalConf.values()) {
                Integer slaveId = ((JSONObject) value).getInteger(KeeperConstants.SLAVE_ID);
                slavaIds.add(slaveId);
            }
        }
        ArrayList<Integer> result = new ArrayList<>();
        for (int i = 0; i < number; i++) {
            Integer slaveId = generateSlaveId(slavaIds);
            slavaIds.add(slaveId);
            result.add(slaveId);
        }
        return result;
    }

    private Integer generateSlaveId(HashSet<Integer> slavaIds) {
        for (int i = 1235; ; i++) {
            if (!slavaIds.contains(i)) {
                return i;
            }
        }
    }

    public JSONObject getCanalConf(String dsName) throws Exception {
        JSONObject canalConfJson = getZKNodeData(Constants.CANAL_PROPERTIES_ROOT);
        JSONObject dsCanalConf = canalConfJson.getJSONObject(KeeperConstants.DS_CANAL_CONF);
        JSONObject canalDeployConf = canalConfJson.getJSONObject(KeeperConstants.DEPLOY_CONF);
        if (dsCanalConf != null) {
            JSONObject canalConf = dsCanalConf.getJSONObject(dsName);
            if (canalConf != null) {
                canalConfJson.put(KeeperConstants.HOST, canalConf.get(KeeperConstants.HOST));
                canalConfJson.put(KeeperConstants.CANAL_ADD, canalConf.get(KeeperConstants.CANAL_ADD));
                canalConfJson.put(KeeperConstants.SLAVE_ID, canalConf.get(KeeperConstants.SLAVE_ID));
                if (canalDeployConf != null) {
                    JSONObject canalHostInfo = canalDeployConf.getJSONObject(canalConfJson.getString(KeeperConstants.HOST));
                    if (canalHostInfo != null) {
                        canalConfJson.put(KeeperConstants.CANAL_PATH, canalHostInfo.getString(KeeperConstants.CANAL_PATH));
                        canalConfJson.put(KeeperConstants.PORT, canalHostInfo.getString(KeeperConstants.PORT));
                        canalConfJson.put(KeeperConstants.USER, canalHostInfo.getString(KeeperConstants.USER));
                    }
                }
            }
        }
        canalConfJson.put("dsName", dsName);
        return canalConfJson;
    }

    public void deleteCanalConf(String dsName) throws Exception {
        JSONObject canalConfJson = getZKNodeData(Constants.CANAL_PROPERTIES_ROOT);
        JSONObject dsCanalConf = canalConfJson.getJSONObject(KeeperConstants.DS_CANAL_CONF);
        if (dsCanalConf != null) {
            dsCanalConf.remove(dsName);
        }
        zkService.setData(Constants.CANAL_PROPERTIES_ROOT,
                JsonFormatUtils.toPrettyFormat(canalConfJson.toJSONString()).getBytes(KeeperConstants.UTF8));
    }

    public void setCanalConf(Map<String, String> map) throws Exception {
        String dsName = map.get("dsName");
        JSONObject canalConfJson = getZKNodeData(Constants.CANAL_PROPERTIES_ROOT);
        String hosts = canalConfJson.getString(KeeperConstants.HOST);
        String host = map.get(KeeperConstants.HOST).trim();
        hosts = StringUtils.isBlank(hosts) ? host : hosts;
        if (!hosts.contains(host)) {
            hosts = hosts + "," + host;
        }
        canalConfJson.put(KeeperConstants.HOSTS, hosts);

        JSONObject canalDeployConf = canalConfJson.getJSONObject(KeeperConstants.DEPLOY_CONF);
        canalDeployConf = canalDeployConf != null ? canalDeployConf : new JSONObject();

        JSONObject canalHostInfo = canalDeployConf.getJSONObject(host);
        canalHostInfo = canalHostInfo != null ? canalHostInfo : new JSONObject();

        if (map.get(KeeperConstants.PORT) != null) {
            canalHostInfo.put(KeeperConstants.PORT, map.get(KeeperConstants.PORT));
        }
        if (map.get(KeeperConstants.USER) != null) {
            canalHostInfo.put(KeeperConstants.USER, map.get(KeeperConstants.USER));
        }
        if (map.get(KeeperConstants.CANAL_PATH) != null) {
            canalHostInfo.put(KeeperConstants.CANAL_PATH, map.get(KeeperConstants.CANAL_PATH));
        }
        canalDeployConf.put(host, canalHostInfo);
        canalConfJson.put(KeeperConstants.DEPLOY_CONF, canalDeployConf);

        JSONObject dsCanalConf = canalConfJson.getJSONObject(KeeperConstants.DS_CANAL_CONF);
        dsCanalConf = dsCanalConf != null ? dsCanalConf : new JSONObject();

        JSONObject canalConf = dsCanalConf.getJSONObject(dsName);
        canalConf = canalConf != null ? canalConf : new JSONObject();

        canalConf.put(KeeperConstants.HOST, host);
        if (StringUtils.isNotBlank(map.get(KeeperConstants.CANAL_ADD))) {
            canalConf.put(KeeperConstants.CANAL_ADD, map.get(KeeperConstants.CANAL_ADD));
        }
        if (StringUtils.isNotBlank(map.get(KeeperConstants.SLAVE_ID))) {
            canalConf.put(KeeperConstants.SLAVE_ID, map.get(KeeperConstants.SLAVE_ID));
        }
        dsCanalConf.put(dsName, canalConf);

        canalConfJson.put(KeeperConstants.DS_CANAL_CONF, dsCanalConf);
        zkService.setData(Constants.CANAL_PROPERTIES_ROOT,
                JsonFormatUtils.toPrettyFormat(canalConfJson.toJSONString()).getBytes(KeeperConstants.UTF8));
    }

    public Boolean isAutoDeployOgg(String dsName) throws Exception {
        if (!zkService.isExists(Constants.OGG_PROPERTIES_ROOT)) {
            return false;
        }
        JSONObject oggConfJson = getZKNodeData(Constants.OGG_PROPERTIES_ROOT);
        JSONObject dsOggConf = oggConfJson.getJSONObject(KeeperConstants.DS_OGG_CONF);
        if (dsOggConf == null) {
            return false;
        }

        if (dsOggConf.getJSONObject(dsName) != null) {
            return true;
        } else {
            return false;
        }
    }

    public Boolean isAutoDeployCanal(String dsName) throws Exception {
        if (!zkService.isExists(Constants.CANAL_PROPERTIES_ROOT)) {
            return false;
        }
        JSONObject canalConfJson = getZKNodeData(Constants.CANAL_PROPERTIES_ROOT);
        JSONObject dsCanalConf = canalConfJson.getJSONObject(KeeperConstants.DS_CANAL_CONF);
        if (dsCanalConf == null) {
            return false;
        }
        if (dsCanalConf.getJSONObject(dsName) != null) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * @param dsName
     * @return
     */
    public int addOracleLine(String dsName) throws Exception {

        Properties properties = zkService.getProperties(Constants.GLOBAL_PROPERTIES_ROOT);
        String bs = properties.getProperty(KeeperConstants.GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS);
        JSONObject oggConf = getOggConf(dsName);
        String host = oggConf.getString(KeeperConstants.HOST);
        String port = oggConf.getString(KeeperConstants.PORT);
        String user = oggConf.getString(KeeperConstants.USER);
        String oggPath = oggConf.getString(KeeperConstants.OGG_PATH);
        String oggToolPath = oggConf.getString(KeeperConstants.OGG_TOOL_PATH);
        String NLS_LANG = oggConf.getString(KeeperConstants.NLS_LANG);
        String trailName = oggConf.getString(KeeperConstants.TRAIL_NAME);
        String replicatName = oggConf.getString(KeeperConstants.REPLICAT_NAME);
        String schemaName = oggConf.getString("schemaName");
        String tableNames = oggConf.getString("tableNames");
        String trailPath = oggPath + "/dirdat/" + trailName;

        checkOggTool(host, port, user, oggToolPath);

        StringBuilder sb = new StringBuilder();
        sb.append("cd ").append(oggToolPath);
        sb.append("; sh addLine.sh '-t newLine ");
        sb.append(" -dn ").append(dsName);
        if (StringUtils.isNotBlank(schemaName)) {
            sb.append(" -sn ").append(schemaName);
        }
        if (StringUtils.isNotBlank(tableNames)) {
            sb.append(" -tn ").append(tableNames);
        }
        sb.append(" -dp ").append(oggPath + "/dirprm");
        sb.append(" -nl ").append(NLS_LANG);
        sb.append(" -rn ").append(replicatName);
        sb.append(" -bs ").append(bs);
        sb.append("'");
        String result = SSHUtils.executeCommand(user, host, Integer.parseInt(port), env.getProperty("pubKeyPath"), sb.toString(), true);
        if (result == null) {
            return MessageCode.SSH_CONF_ERROR;
        }
        if (StringUtils.isNotBlank(result)) {
            return MessageCode.DEPLOY_REPLICAT_ERROR;
        }
        int addReplicatCode = addReplicat(user, host, port, replicatName, trailPath, oggPath);
        if (0 != addReplicatCode) {
            return addReplicatCode;
        }

        int startCode = startReplicat(user, host, port, replicatName, oggPath);
        if (0 != startCode) {
            return startCode;
        }
        return 0;
    }

    private void checkOggTool(String host, String port, String user, String oggToolPath) {
        String cmd = String.format("ls %s | grep 'addLine.sh'", oggToolPath);
        String result = SSHUtils.executeCommand(user, host, Integer.parseInt(port), env.getProperty("pubKeyPath"), cmd, false);
        if (StringUtils.isBlank(result)) {
            logger.info("directory {} is not exist", oggToolPath);
            result = SSHUtils.executeCommand(user, host, Integer.parseInt(port), env.getProperty("pubKeyPath"), "mkdir -pv " + oggToolPath, true);
            if (StringUtils.isNotBlank(result)) {
                logger.error("自动创建ogg部署目录失败!");
                throw new RuntimeException(result);
            }
            result = SSHUtils.uploadFile(user, host, Integer.parseInt(port), env.getProperty("pubKeyPath"), "../zip/" + KeeperConstants.DBUS_OGG_AUTO_ZIP, oggToolPath);
            if (!StringUtils.equals("ok", result)) {
                logger.error("自动上传ogg部署小具失败!");
                throw new RuntimeException(result);
            }

            cmd = String.format("cd %s; unzip %s; mv %s/* ./; rm -rf %s", oggToolPath, KeeperConstants.DBUS_OGG_AUTO_ZIP,
                    KeeperConstants.DBUS_OGG_AUTO, KeeperConstants.DBUS_OGG_AUTO);
            result = SSHUtils.executeCommand(user, host, Integer.parseInt(port), env.getProperty("pubKeyPath"), cmd, true);
            if (StringUtils.isNotBlank(result)) {
                logger.error("解压ogg部署小具失败!");
                throw new RuntimeException(result);
            }
        }
    }


    public int addOracleSchema(String dsName, String schemaName, String tableNames) throws Exception {
        JSONObject oggConf = getOggConf(dsName);
        String host = oggConf.getString(KeeperConstants.HOST);
        String port = oggConf.getString(KeeperConstants.PORT);
        String user = oggConf.getString(KeeperConstants.USER);
        String oggPath = oggConf.getString(KeeperConstants.OGG_PATH);
        String oggToolPath = oggConf.getString(KeeperConstants.OGG_TOOL_PATH);
        String replicatName = oggConf.getString(KeeperConstants.REPLICAT_NAME);

        int stopCode = stopReplicat(user, host, port, replicatName, oggPath);
        if (0 != stopCode) {
            return stopCode;
        }

        StringBuilder sb = new StringBuilder();

        sb.append("cd ").append(oggToolPath);
        sb.append("; sh addLine.sh '-t newSchema ");
        sb.append(" -sn ").append(schemaName);
        if (StringUtils.isNotBlank(tableNames)) {
            sb.append(" -tn ").append(tableNames);
        }
        sb.append(" -dp ").append(oggPath + "/dirprm");
        sb.append(" -rn ").append(replicatName);
        sb.append("'");

        String result = SSHUtils.executeCommand(user, host, Integer.parseInt(port), env.getProperty("pubKeyPath"), sb.toString(), true);
        if (StringUtils.isNotBlank(result)) {
            return MessageCode.DEPLOY_REPLICAT_ERROR;
        }
        int startCode = startReplicat(user, host, port, replicatName, oggPath);
        if (0 != startCode) {
            return startCode;
        }
        return 0;
    }

    /**
     * @param dsName
     * @return
     */
    public int deleteOracleLine(String dsName) throws Exception {
        JSONObject oggConf = getOggConf(dsName);
        String host = oggConf.getString(KeeperConstants.HOST);
        String port = oggConf.getString(KeeperConstants.PORT);
        String user = oggConf.getString(KeeperConstants.USER);
        String oggPath = oggConf.getString(KeeperConstants.OGG_PATH);
        String oggToolPath = oggConf.getString(KeeperConstants.OGG_TOOL_PATH);
        String replicatName = oggConf.getString(KeeperConstants.REPLICAT_NAME);

        int stopCode = stopReplicat(user, host, port, replicatName, oggPath);
        if (0 != stopCode) {
            return stopCode;
        }
        StringBuilder sb = new StringBuilder();

        sb.append("cd ").append(oggToolPath);
        sb.append("; sh delLine.sh '-t delLine ");
        sb.append(" -dp ").append(oggPath + "/dirprm");
        sb.append(" -rn ").append(replicatName);
        sb.append("'");
        String result = SSHUtils.executeCommand(user, host, Integer.parseInt(port), env.getProperty("pubKeyPath"), sb.toString(), true);
        if (result == null) {
            return MessageCode.SSH_CONF_ERROR;
        }
        if (StringUtils.isNotBlank(result)) {
            return MessageCode.DEPLOY_REPLICAT_ERROR;
        }

        int deleteCode = deleteReplicat(user, host, port, replicatName, oggPath);
        if (0 != deleteCode) {
            return deleteCode;
        }
        deleteOggConf(dsName);
        return 0;
    }

    public int deleteOracleSchema(String dsName, String schemaName) throws Exception {
        JSONObject oggConf = getOggConf(dsName);
        String host = oggConf.getString(KeeperConstants.HOST);
        String port = oggConf.getString(KeeperConstants.PORT);
        String user = oggConf.getString(KeeperConstants.USER);
        String oggPath = oggConf.getString(KeeperConstants.OGG_PATH);
        String oggToolPath = oggConf.getString(KeeperConstants.OGG_TOOL_PATH);
        String replicatName = oggConf.getString(KeeperConstants.REPLICAT_NAME);

        int stopCode = stopReplicat(user, host, port, replicatName, oggPath);
        if (0 != stopCode) {
            return stopCode;
        }

        StringBuilder sb = new StringBuilder();

        sb.append("cd ").append(oggToolPath);
        sb.append("; sh delLine.sh '-t delSchema ");
        sb.append(" -sn ").append(schemaName);
        sb.append(" -dp ").append(oggPath + "/dirprm");
        sb.append(" -rn ").append(replicatName);
        sb.append("'");

        String result = SSHUtils.executeCommand(user, host, Integer.parseInt(port), env.getProperty("pubKeyPath"), sb.toString(), true);
        if (result == null) {
            return MessageCode.SSH_CONF_ERROR;
        }
        if (StringUtils.isNotBlank(result)) {
            return MessageCode.DEPLOY_REPLICAT_ERROR;
        }
        int startCode = startReplicat(user, host, port, replicatName, oggPath);
        if (0 != startCode) {
            return startCode;
        }
        return 0;
    }

    public int deleteOracleTable(String dsName, String schemaName, String tableNames) throws Exception {
        JSONObject oggConf = getOggConf(dsName);
        String host = oggConf.getString(KeeperConstants.HOST);
        String port = oggConf.getString(KeeperConstants.PORT);
        String user = oggConf.getString(KeeperConstants.USER);
        String oggPath = oggConf.getString(KeeperConstants.OGG_PATH);
        String oggToolPath = oggConf.getString(KeeperConstants.OGG_TOOL_PATH);
        String replicatName = oggConf.getString(KeeperConstants.REPLICAT_NAME);

        int stopCode = stopReplicat(user, host, port, replicatName, oggPath);
        if (0 != stopCode) {
            return stopCode;
        }

        StringBuilder sb = new StringBuilder();

        sb.append("cd ").append(oggToolPath);
        sb.append("; sh delLine.sh '-t delTable ");
        sb.append(" -sn ").append(schemaName);
        sb.append(" -tn ").append(tableNames);
        sb.append(" -dp ").append(oggPath + "/dirprm");
        sb.append(" -rn ").append(replicatName);
        sb.append("'");

        String result = SSHUtils.executeCommand(user, host, Integer.parseInt(port), env.getProperty("pubKeyPath"), sb.toString(), true);
        if (result == null) {
            return MessageCode.SSH_CONF_ERROR;
        }
        if (StringUtils.isNotBlank(result)) {
            return MessageCode.DEPLOY_REPLICAT_ERROR;
        }
        int startCode = startReplicat(user, host, port, replicatName, oggPath);
        if (0 != startCode) {
            return startCode;
        }
        return 0;
    }

    public int addReplicat(String user, String host, String port, String replicatName, String trailName, String
            oggPath) {
        //add replicat oratest, exttrail /u01/golden123111/dirdat/ab
        String command = "(echo add replicat " + replicatName + ",exttrail " + trailName + ";echo exit)|" + oggPath + "/ggsci";
        String result = SSHUtils.executeCommand(user, host, Integer.parseInt(port), env.getProperty("pubKeyPath"), command, true);
        if (result == null) {
            return MessageCode.SSH_CONF_ERROR;
        }
        if (StringUtils.isNotBlank(result)) {
            return MessageCode.ADD_REPLICAT_ERROR;
        }
        return 0;
    }

    public int deleteReplicat(String user, String host, String port, String replicatName, String oggPath) {
        //delete oratest
        String command = "(echo delete replicat " + replicatName + ";echo exit)|" + oggPath + "/ggsci";
        String result = SSHUtils.executeCommand(user, host, Integer.parseInt(port), env.getProperty("pubKeyPath"), command, true);
        if (result == null) {
            return MessageCode.SSH_CONF_ERROR;
        }
        if (StringUtils.isNotBlank(result)) {
            return MessageCode.ADD_REPLICAT_ERROR;
        }
        return 0;
    }

    public int stopReplicat(String user, String host, String port, String replicatName, String oggPath) {
        String command = "(echo stop replicat " + replicatName + ";echo exit)|" + oggPath + "/ggsci";
        String result = SSHUtils.executeCommand(user, host, Integer.parseInt(port), env.getProperty("pubKeyPath"), command, true);
        if (result == null) {
            return MessageCode.SSH_CONF_ERROR;
        }
        if (StringUtils.isNotBlank(result)) {
            return MessageCode.STOP_REPLICAT_ERROR;
        }
        return 0;
    }

    public int startReplicat(String user, String host, String port, String replicatName, String oggPath) {
        String command = "(echo start replicat " + replicatName + ";echo exit)|" + oggPath + "/ggsci";
        String result = SSHUtils.executeCommand(user, host, Integer.parseInt(port), env.getProperty("pubKeyPath"), command, true);
        if (StringUtils.isNotBlank(result)) {
            return MessageCode.START_REPLICAT_ERROR;
        }
        return 0;
    }

    /**
     * @param dsName
     * @return
     */
    public int addCanalLine(String dsName, String canalUser, String canalPass) throws Exception {
        JSONObject canalConf = getCanalConf(dsName);
        String host = canalConf.getString(KeeperConstants.HOST);
        String port = canalConf.getString(KeeperConstants.PORT);
        String user = canalConf.getString(KeeperConstants.USER);
        String canalAdd = canalConf.getString(KeeperConstants.CANAL_ADD);
        String zkString = env.getProperty("zk.str");
        String canalPath = canalConf.getString(KeeperConstants.CANAL_PATH);

        Properties properties = zkService.getProperties(Constants.GLOBAL_PROPERTIES_ROOT);
        String bs = properties.getProperty(KeeperConstants.GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS);

        checkCanalTool(host, port, user, canalPath);
        Integer slaveId = getCanalSlaveId(1).get(0);

        StringBuilder sb = new StringBuilder();
        sb.append("cd ").append(canalPath);
        sb.append(";sh addLine.sh '");
        sb.append(" -dn ").append(dsName);
        sb.append(" -zk ").append(zkString);
        sb.append(" -a ").append(canalAdd);
        sb.append(" -u ").append(canalUser);
        sb.append(" -p ").append(canalPass);
        sb.append(" -s ").append(slaveId);
        sb.append(" -bs ").append(bs);
        sb.append("'");
        String result = SSHUtils.executeCommand(user, host, Integer.parseInt(port), env.getProperty("pubKeyPath"), sb.toString(), true);
        if (result == null) {
            return MessageCode.SSH_CONF_ERROR;
        }
        if (StringUtils.isNotBlank(result)) {
            return MessageCode.DEPLOY_CANAL_ERROR;
        }
        //部署完成保存salveId
        HashMap<String, String> map = new HashMap<>();
        map.put("dsName", dsName);
        map.put("host", host);
        map.put(KeeperConstants.SLAVE_ID, slaveId.toString());
        setCanalConf(map);
        return 0;
    }

    private void checkCanalTool(String host, String port, String user, String canalPath) {
        String cmd = String.format("ls %s | grep 'addLine.sh'", canalPath);
        String result = SSHUtils.executeCommand(user, host, Integer.parseInt(port), env.getProperty("pubKeyPath"), cmd, false);
        if (StringUtils.isBlank(result)) {
            logger.info("directory {} is not exist", canalPath);
            result = SSHUtils.executeCommand(user, host, Integer.parseInt(port), env.getProperty("pubKeyPath"), "mkdir -pv " + canalPath, true);
            if (StringUtils.isNotBlank(result)) {
                logger.error("自动创建canal部署目录失败!");
                throw new RuntimeException(result);
            }
            result = SSHUtils.uploadFile(user, host, Integer.parseInt(port), env.getProperty("pubKeyPath"), "../zip/" + KeeperConstants.DBUS_CANAL_AUTO_ZIP, canalPath);
            if (!StringUtils.equals("ok", result)) {
                logger.error("自动上传canal部署小具失败!");
                throw new RuntimeException(result);
            }

            cmd = String.format("cd %s; unzip %s; mv %s/* ./; rm -rf %s", canalPath, KeeperConstants.DBUS_CANAL_AUTO_ZIP,
                    KeeperConstants.DBUS_CANAL_AUTO, KeeperConstants.DBUS_CANAL_AUTO);

            result = SSHUtils.executeCommand(user, host, Integer.parseInt(port), env.getProperty("pubKeyPath"), cmd, true);
            if (StringUtils.isNotBlank(result)) {
                logger.error("解压canal部署小具失败!");
                throw new RuntimeException(result);
            }

            result = SSHUtils.uploadFile(user, host, Integer.parseInt(port), env.getProperty("pubKeyPath"),
                    "../zip/" + KeeperConstants.CANAL_ZIP, canalPath);
            if (!StringUtils.equals("ok", result)) {
                logger.error("自动上传canal默认包失败!");
                throw new RuntimeException(result);
            }
            cmd = String.format("cd %s; unzip %s", canalPath, KeeperConstants.CANAL_ZIP);
            result = SSHUtils.executeCommand(user, host, Integer.parseInt(port), env.getProperty("pubKeyPath"), cmd, true);
            if (StringUtils.isNotBlank(result)) {
                logger.error("解压canal默认包失败!");
                throw new RuntimeException(result);
            }
            logger.error("自动上传canal工具包成功.");
        }
    }

    /**
     * @param dsName
     * @return
     */
    public int delCanalLine(String dsName) throws Exception {
        JSONObject canalConf = getCanalConf(dsName);
        String host = canalConf.getString(KeeperConstants.HOST);
        String port = canalConf.getString(KeeperConstants.PORT);
        String user = canalConf.getString(KeeperConstants.USER);

        String canalPath = canalConf.getString(KeeperConstants.CANAL_PATH);

        StringBuilder sb = new StringBuilder();
        sb.append("cd ").append(canalPath);
        sb.append(";sh delLine.sh '");
        sb.append(" -dn ").append(dsName);
        sb.append("'");

        String result = SSHUtils.executeCommand(user, host, Integer.parseInt(port), env.getProperty("pubKeyPath"), sb.toString(), true);
        if (result == null) {
            return MessageCode.SSH_CONF_ERROR;
        }
        if (StringUtils.isNotBlank(result)) {
            return MessageCode.DEPLOY_CANAL_ERROR;
        }

        deleteCanalConf(dsName);
        return 0;
    }

    public String getOggReplicatStatus(String dsName) throws Exception {
        JSONObject oggConf = getOggConf(dsName);
        String host = oggConf.getString(KeeperConstants.HOST);
        String replicatName = oggConf.getString(KeeperConstants.REPLICAT_NAME);
        if (StringUtils.isBlank(host) || StringUtils.isBlank(replicatName)) {
            return null;
        }
        String user = oggConf.getString(KeeperConstants.USER);
        Integer port = oggConf.getInteger(KeeperConstants.PORT);
        String oggPath = oggConf.getString(KeeperConstants.OGG_PATH);

        String command = "(echo info all;echo exit)|" + oggPath + "/ggsci";
        String pubKeyPath = env.getProperty("pubKeyPath");
        String result = SSHUtils.executeCommand(user, host, port, pubKeyPath, command, false);
        String[] split = result.split("\\n");
        String line = "";
        for (int i = 0; i < split.length; i++) {
            line = split[i];
            if (StringUtils.isNoneBlank(line) && line.contains("REPLICAT")) {
                while (line.contains("  ")) {
                    line = line.replace("  ", " ");
                }
                String[] s = line.split(" ");
                if (replicatName.equalsIgnoreCase(s[2])) {
                    return s[1];
                }
            }
        }
        return null;
    }

    public ArrayList<HashMap<String, String>> getOggReplicatStatus(List<DataSource> dataSourceList) throws
            Exception {
        //1.获取所有ogg replicat信息
        JSONObject oggConfJson = this.getOggConf();
        HashMap<String, HashMap<String, String>> replicats = new HashMap<>();
        HashSet<String> hosts = new HashSet<>();
        JSONObject dsOggConf = oggConfJson.getJSONObject(KeeperConstants.DS_OGG_CONF);
        if (dsOggConf != null) {
            for (Map.Entry<String, Object> entry : dsOggConf.entrySet()) {
                JSONObject value = (JSONObject) entry.getValue();
                HashMap<String, String> oggConfMap = new HashMap<>();
                oggConfMap.put("dsName", entry.getKey());
                oggConfMap.put(KeeperConstants.REPLICAT_NAME, value.getString(KeeperConstants.REPLICAT_NAME));
                oggConfMap.put(KeeperConstants.TRAIL_NAME, value.getString(KeeperConstants.TRAIL_NAME));
                oggConfMap.put(KeeperConstants.HOST, value.getString(KeeperConstants.HOST));
                oggConfMap.put(KeeperConstants.USER, value.getString(KeeperConstants.USER));
                replicats.put(value.getString(KeeperConstants.REPLICAT_NAME), oggConfMap);
                hosts.add(value.getString(KeeperConstants.HOST));
            }
        }
        //2.获取所有ogg replicat进程状态信息
        JSONObject oggDeployConf = oggConfJson.getJSONObject(KeeperConstants.DEPLOY_CONF);
        Map<String, Map<String, String>> replicatState = getAllOggInfos(hosts, oggDeployConf);

        //3.状态整理
        HashMap<String, HashMap<String, String>> statusMap = new HashMap<>();
        for (Map.Entry<String, HashMap<String, String>> entry : replicats.entrySet()) {
            String replicatName = entry.getKey();
            HashMap<String, String> value = entry.getValue();

            Map<String, String> replicat = replicatState.get(replicatName.toUpperCase());
            if (replicat == null) {
                logger.info("replicat {} not exist.", replicatName);
                continue;
            }
            String state = value.put("state", replicat.get("state"));
            statusMap.put(value.get("dsName"), value);
        }
        //4.状态信息处理
        ArrayList<HashMap<String, String>> oggReplicatStatus = new ArrayList<>();
        for (DataSource ds : dataSourceList) {
            HashMap<String, String> map = statusMap.get(ds.getDsName());
            if (map == null) {
                map = new HashMap<>();
                map.put("dsName", ds.getDsName());
                map.put("state", "error");
            }
            oggReplicatStatus.add(map);
        }
        return oggReplicatStatus;
    }

    public JSONObject getOggConf() throws Exception {
        return getZKNodeData(Constants.OGG_PROPERTIES_ROOT);
    }

    public JSONObject getCanalConf() throws Exception {
        return getZKNodeData(Constants.CANAL_PROPERTIES_ROOT);
    }

    public String getCanalPid(String dsName) throws Exception {
        JSONObject canalConf = getCanalConf(dsName);
        String host = canalConf.getString(KeeperConstants.HOST);
        if (StringUtils.isBlank(host)) {
            return null;
        }
        String user = canalConf.getString(KeeperConstants.USER);
        Integer port = canalConf.getInteger(KeeperConstants.PORT);
        String command = "ps -ef | grep 'canal-" + dsName + "/' | grep -v grep | awk '{print $2}'";
        String pubKeyPath = env.getProperty("pubKeyPath");
        return SSHUtils.executeCommand(user, host, port, pubKeyPath, command, false);
    }

    public ArrayList<HashMap<String, String>> getCanalStatus(List<DataSource> dataSourceList) throws Exception {
        //1.获取所有canal信息
        JSONObject canalConfJson = this.getCanalConf();
        HashMap<String, HashMap<String, String>> canals = new HashMap<>();
        HashSet<String> hosts = new HashSet<>();
        JSONObject dsCanalConf = canalConfJson.getJSONObject(KeeperConstants.DS_CANAL_CONF);
        if (dsCanalConf != null) {
            for (Map.Entry<String, Object> entry : dsCanalConf.entrySet()) {
                JSONObject value = (JSONObject) entry.getValue();
                HashMap<String, String> canalConfMap = new HashMap<>();
                canalConfMap.put("dsName", entry.getKey());
                canalConfMap.put(KeeperConstants.HOST, value.getString(KeeperConstants.HOST));
                canalConfMap.put(KeeperConstants.CANAL_ADD, value.getString(KeeperConstants.CANAL_ADD));
                canalConfMap.put(KeeperConstants.SLAVE_ID, value.getString(KeeperConstants.SLAVE_ID));
                canals.put(entry.getKey(), canalConfMap);
                hosts.add(value.getString(KeeperConstants.HOST));
            }
        }
        //2.获取所有canal进程状态信息
        Set<String> canalResults = getAllCanalInfos(canalConfJson.getJSONObject(KeeperConstants.DEPLOY_CONF), hosts).keySet();
        //3.状态信息处理
        ArrayList<HashMap<String, String>> canalStatus = new ArrayList<>();
        for (DataSource ds : dataSourceList) {
            HashMap<String, String> canal = new HashMap<>();
            String dsName = ds.getDsName();
            canal.put("dsName", dsName);
            HashMap<String, String> map = canals.get(dsName);
            if (map != null) {
                canal.putAll(map);
            }
            for (String line : canalResults) {
                if (line.contains("canal-" + dsName + "/")) {
                    int count = 0;
                    for (String s : line.split(" ")) {
                        if (StringUtils.isNoneBlank(s) && ++count == 2) {
                            canal.put("pid", s);
                        }
                    }
                }
            }
            if (StringUtils.isNotBlank(canal.get("pid"))) {
                canal.put("state", "ok");
            } else {
                canal.put("state", "error");
            }
            canalStatus.add(canal);
        }
        return canalStatus;
    }

    public List<DeployInfoBean> getOggCanalDeployInfo() throws Exception {
        HashMap<String, DeployInfoBean> deployInfoMap = new HashMap<>();
        JSONObject oggConfJson = getZKNodeData(Constants.OGG_PROPERTIES_ROOT);
        JSONObject dsOggConf = oggConfJson.getJSONObject(KeeperConstants.DS_OGG_CONF);
        JSONObject oggDeployConf = oggConfJson.getJSONObject(KeeperConstants.DEPLOY_CONF);
        if (oggDeployConf != null) {
            for (Map.Entry<String, Object> entry : oggDeployConf.entrySet()) {
                String host = entry.getKey();
                JSONObject value = (JSONObject) entry.getValue();
                DeployInfoBean info = new DeployInfoBean();
                info.setHost(host);
                info.setMaxReplicatNumber(value.getIntValue(KeeperConstants.MAX_REPLICAT_NUMBER));
                info.setMgrReplicatPort(value.getInteger(KeeperConstants.MGR_REPLICAT_PORT));
                info.setOggTrailPath(value.getString(KeeperConstants.OGG_TRAIL_PATH));
                info.setOggToolPath(value.getString(KeeperConstants.OGG_TOOL_PATH));
                info.setOggPath(value.getString(KeeperConstants.OGG_PATH));
                deployInfoMap.put(host, info);
            }
        }

        if (dsOggConf != null) {
            Collection<Object> values = dsOggConf.values();
            for (Object obj : values) {
                JSONObject value = (JSONObject) obj;
                String host = value.getString(KeeperConstants.HOST);
                DeployInfoBean info = deployInfoMap.get(host);
                if (info == null) {
                    info = new DeployInfoBean();
                    info.setHost(host);
                }
                int deployReplicatNumber = info.getDeployReplicatNumber();
                info.setDeployReplicatNumber(++deployReplicatNumber);
                deployInfoMap.put(host, info);
            }
        }

        JSONObject canalConfJson = getZKNodeData(Constants.CANAL_PROPERTIES_ROOT);
        JSONObject dsCanalConf = canalConfJson.getJSONObject(KeeperConstants.DS_CANAL_CONF);
        JSONObject canalDeployConf = canalConfJson.getJSONObject(KeeperConstants.DEPLOY_CONF);

        if (canalDeployConf != null) {
            for (Map.Entry<String, Object> entry : canalDeployConf.entrySet()) {
                String host = entry.getKey();
                JSONObject value = (JSONObject) entry.getValue();
                DeployInfoBean info = deployInfoMap.get(host);
                if (info == null) {
                    info = new DeployInfoBean();
                }
                info.setHost(host);
                info.setMaxCanalNumber(value.getIntValue(KeeperConstants.MAX_CANAL_NUMBER));
                info.setCanalPath(value.getString(KeeperConstants.CANAL_PATH));
                deployInfoMap.put(host, info);
            }
        }
        if (dsCanalConf != null) {
            Collection<Object> values = dsCanalConf.values();
            for (Object obj : values) {
                JSONObject value = (JSONObject) obj;
                String host = value.getString(KeeperConstants.HOST);
                DeployInfoBean info = deployInfoMap.get(host);
                if (info == null) {
                    info = new DeployInfoBean();
                    info.setHost(host);
                }
                int deployCanalNumber = info.getDeployCanalNumber();
                info.setDeployCanalNumber(++deployCanalNumber);
                deployInfoMap.put(host, info);
            }
        }
        ArrayList<DeployInfoBean> deployInfoBeans = new ArrayList<>(deployInfoMap.values());
        Collections.sort(deployInfoBeans, (o1, o2) -> o1.getHost().compareTo(o2.getHost()));
        return deployInfoBeans;
    }

    public void updateOggCanalDeployInfo(DeployInfoBean deployInfoBean) throws Exception {
        JSONObject oggConfJson = getZKNodeData(Constants.OGG_PROPERTIES_ROOT);
        JSONObject oggDeployConf = oggConfJson.getJSONObject(KeeperConstants.DEPLOY_CONF);
        oggDeployConf = oggDeployConf == null ? new JSONObject() : oggDeployConf;
        JSONObject oggHostInfo = oggDeployConf.getJSONObject(deployInfoBean.getHost());
        oggHostInfo = oggHostInfo == null ? new JSONObject() : oggHostInfo;
        oggHostInfo.put(KeeperConstants.MAX_REPLICAT_NUMBER, deployInfoBean.getMaxReplicatNumber());
        oggHostInfo.put(KeeperConstants.MGR_REPLICAT_PORT, deployInfoBean.getMgrReplicatPort());
        oggHostInfo.put(KeeperConstants.OGG_TRAIL_PATH, deployInfoBean.getOggTrailPath());
        oggHostInfo.put(KeeperConstants.OGG_TOOL_PATH, deployInfoBean.getOggToolPath());
        oggHostInfo.put(KeeperConstants.OGG_PATH, deployInfoBean.getOggPath());
        oggDeployConf.put(deployInfoBean.getHost(), oggHostInfo);
        oggConfJson.put(KeeperConstants.DEPLOY_CONF, oggDeployConf);
        zkService.setData(Constants.OGG_PROPERTIES_ROOT, JsonFormatUtils.toPrettyFormat(oggConfJson.toJSONString()).getBytes(KeeperConstants.UTF8));

        JSONObject canalConfJson = getZKNodeData(Constants.CANAL_PROPERTIES_ROOT);
        JSONObject canalDeployConf = canalConfJson.getJSONObject(KeeperConstants.DEPLOY_CONF);
        canalDeployConf = canalDeployConf == null ? new JSONObject() : canalDeployConf;
        JSONObject canalHostInfo = canalDeployConf.getJSONObject(deployInfoBean.getHost());
        canalHostInfo = canalHostInfo == null ? new JSONObject() : canalHostInfo;
        canalHostInfo.put(KeeperConstants.MAX_CANAL_NUMBER, deployInfoBean.getMaxCanalNumber());
        canalHostInfo.put(KeeperConstants.CANAL_PATH, deployInfoBean.getCanalPath());
        canalDeployConf.put(deployInfoBean.getHost(), canalHostInfo);
        canalConfJson.put(KeeperConstants.DEPLOY_CONF, canalDeployConf);
        zkService.setData(Constants.CANAL_PROPERTIES_ROOT, JsonFormatUtils.toPrettyFormat(canalConfJson.toJSONString()).getBytes(KeeperConstants.UTF8));
    }

    public int syncOggCanalDeployInfo() throws Exception {
        List<DataSource> list = toolSetService.getDataSourceByDsTypes(Arrays.asList(new String[]{"oracle", "mysql"}));
        List<String> mysql = list.stream().filter(dataSource -> dataSource.getDsType().equals("mysql"))
                .map(DataSource::getDsName).collect(Collectors.toList());
        //获取并更新所有canal部署信息
        JSONObject canalConfJson = this.getCanalConf();
        HashMap<String, HashMap<String, String>> canals = new HashMap<>();
        Set<String> hosts = new HashSet<>();
        JSONObject deployConf = canalConfJson.getJSONObject(KeeperConstants.DEPLOY_CONF);
        if (deployConf != null) {
            hosts = deployConf.keySet();
        }
        JSONObject dsCanalConf = canalConfJson.getJSONObject(KeeperConstants.DS_CANAL_CONF);
        if (dsCanalConf != null) {
            //去除不存在的配置
            Iterator<Map.Entry<String, Object>> iterator = dsCanalConf.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> next = iterator.next();
                if (!mysql.contains(next.getKey())) {
                    iterator.remove();
                }
            }
            Map<String, String> canalResults = getAllCanalInfos(canalConfJson.getJSONObject(KeeperConstants.DEPLOY_CONF), hosts);
            for (Map.Entry<String, String> entry : canalResults.entrySet()) {
                String info = entry.getKey();
                String host = entry.getValue();
                for (Map.Entry<String, Object> conf : dsCanalConf.entrySet()) {
                    String dsName = conf.getKey();
                    JSONObject value = (JSONObject) conf.getValue();
                    // 找到对应的数据线的canal进程
                    if (info.contains("canal-" + dsName + "/")) {
                        value.put(KeeperConstants.HOST, host);
                    }
                }
            }
        }
        zkService.setData(Constants.CANAL_PROPERTIES_ROOT,
                JsonFormatUtils.toPrettyFormat(canalConfJson.toJSONString()).getBytes(KeeperConstants.UTF8));

        List<String> oracle = list.stream().filter(dataSource -> dataSource.getDsType().equals("oracle"))
                .map(DataSource::getDsName).collect(Collectors.toList());
        //获取并更新所有ogg replicat部署信息
        JSONObject oggConfJson = this.getOggConf();
        hosts.clear();
        deployConf = oggConfJson.getJSONObject(KeeperConstants.DEPLOY_CONF);
        if (deployConf != null) {
            hosts = deployConf.keySet();
        }
        JSONObject dsOggConf = oggConfJson.getJSONObject(KeeperConstants.DS_OGG_CONF);
        if (dsOggConf != null) {
            //去除不存在的配置
            Iterator<Map.Entry<String, Object>> iterator = dsOggConf.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> next = iterator.next();
                if (!oracle.contains(next.getKey())) {
                    iterator.remove();
                }
            }
            JSONObject oggDeployConf = oggConfJson.getJSONObject(KeeperConstants.DEPLOY_CONF);
            Map<String, Map<String, String>> replicats = getAllOggInfos(hosts, oggDeployConf);
            for (Map.Entry<String, Object> conf : dsOggConf.entrySet()) {
                JSONObject value = (JSONObject) conf.getValue();
                Map<String, String> replicat = replicats.get(value.getString(KeeperConstants.REPLICAT_NAME).toUpperCase());
                if (replicat == null) {
                    logger.info("replicat {} not exist.", value.getString(KeeperConstants.REPLICAT_NAME));
                    continue;
                }
                value.put(KeeperConstants.HOST, replicat.get("host"));
            }
        }
        zkService.setData(Constants.OGG_PROPERTIES_ROOT,
                JsonFormatUtils.toPrettyFormat(oggConfJson.toJSONString()).getBytes(KeeperConstants.UTF8));
        return 0;
    }

    private Map<String, Map<String, String>> getAllOggInfos(Set<String> hosts, JSONObject oggDeployConf) {
        String pubKeyPath = env.getProperty("pubKeyPath");
        Map<String, Map<String, String>> replicats = new HashMap<>();
        for (String host : hosts) {
            JSONObject hostInfo = oggDeployConf.getJSONObject(host);
            if (hostInfo != null) {
                Integer port = hostInfo.getInteger(KeeperConstants.PORT);
                String user = hostInfo.getString(KeeperConstants.USER);
                String oggPath = hostInfo.getString(KeeperConstants.OGG_PATH);
                String command = "(echo info all;echo exit)|" + oggPath + "/ggsci";
                String result = SSHUtils.executeCommand(user, host, port, pubKeyPath, command, false);
                String[] split = result.split("\\n");
                String line = "";
                for (int i = 0; i < split.length; i++) {
                    HashMap<String, String> replicat = new HashMap<>();
                    line = split[i];
                    if (StringUtils.isNoneBlank(line) && line.contains("REPLICAT")) {
                        while (line.contains("  ")) {
                            line = line.replace("  ", " ");
                        }
                        String[] s = line.split(" ");
                        replicat.put("host", host);
                        replicat.put("replicat", s[2]);
                        replicat.put("state", s[1]);
                        replicats.put(s[2], replicat);
                    }
                }
            }
        }
        return replicats;
    }

    private Map<String, String> getAllCanalInfos(JSONObject canalDeployConf, Set<String> hosts) {
        //获取所有canal进程状态信息
        String command = "ps -ef | grep 'canal' | grep -v grep";
        String pubKeyPath = env.getProperty("pubKeyPath");
        Map<String, String> canalResults = new HashMap<>();
        for (String host : hosts) {
            JSONObject hostInfo = canalDeployConf.getJSONObject(host);
            if (hostInfo != null) {
                String user = hostInfo.getString(KeeperConstants.USER);
                Integer port = hostInfo.getInteger(KeeperConstants.PORT);
                String result = SSHUtils.executeCommand(user, host, port, pubKeyPath, command, false);
                for (String line : result.split("\\n")) {
                    canalResults.put(line, host);
                }
            }
        }
        return canalResults;
    }

    /**
     * @param type(editFilter,initFilter,deleteFilter)
     * @param dsName
     * @param tableNames
     * @return
     * @throws Exception
     */
    public int editCanalFilter(String type, String dsName, String tableNames) throws Exception {
        JSONObject canalConf = getCanalConf(dsName);
        String host = canalConf.getString(KeeperConstants.HOST);
        String port = canalConf.getString(KeeperConstants.PORT);
        String user = canalConf.getString(KeeperConstants.USER);
        String canalPath = canalConf.getString(KeeperConstants.CANAL_PATH);

        checkCanalTool(host, port, user, canalPath);

        // 添加心跳表
        if (!type.equals("deleteFilter") && !tableNames.contains("dbus.db_heartbeat_monitor")) {
            tableNames = "dbus.db_heartbeat_monitor," + tableNames;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("cd ").append(canalPath);
        sb.append(";sh addLine.sh '-t ").append(type);
        sb.append(" -dn ").append(dsName);
        sb.append(" -tn ").append(tableNames);
        sb.append("'");
        String result = SSHUtils.executeCommand(user, host, Integer.parseInt(port), env.getProperty("pubKeyPath"), sb.toString(), true);
        if (result == null) {
            return MessageCode.SSH_CONF_ERROR;
        }
        if (StringUtils.isNotBlank(result)) {
            return MessageCode.DEPLOY_CANAL_ERROR;
        }
        return 0;
    }

}
