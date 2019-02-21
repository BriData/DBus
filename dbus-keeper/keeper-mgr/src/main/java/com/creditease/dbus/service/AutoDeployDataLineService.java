package com.creditease.dbus.service;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.utils.JsonFormatUtils;
import com.creditease.dbus.utils.SSHUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

/**
 * This is Description
 *
 * @author xiancangao
 * @date 2018/12/11
 */
@Service
public class AutoDeployDataLineService {

    @Autowired
    private IZkService zkService;
    @Autowired
    private Environment env;


    public JSONObject getOggConf(String dsName) throws Exception {
        JSONObject oggConfJson = new JSONObject();
        if (zkService.isExists(Constants.OGG_PROPERTIES_ROOT)) {
            oggConfJson = JSONObject.parseObject(new String(zkService.getData(Constants.OGG_PROPERTIES_ROOT), KeeperConstants.UTF8));
        }
        JSONObject dsOggConf = oggConfJson.getJSONObject(KeeperConstants.DS_OGG_CONF);
        if (dsOggConf != null) {
            JSONObject oggConf = dsOggConf.getJSONObject(dsName);
            if (oggConf != null) {
                oggConfJson.put(KeeperConstants.REPLICAT_NAME, oggConf.getString(KeeperConstants.REPLICAT_NAME));
                oggConfJson.put(KeeperConstants.TRAIL_NAME, oggConf.getString(KeeperConstants.TRAIL_NAME));
                oggConfJson.put(KeeperConstants.HOST, oggConf.getString(KeeperConstants.HOST));
                oggConfJson.put(KeeperConstants.NLS_LANG, oggConf.getString(KeeperConstants.NLS_LANG));
            }
        }
        return oggConfJson;
    }

    public void setOggConf(Map<String, String> map) throws Exception {
        String dsName = map.get("dsName");

        JSONObject oggConfJson = new JSONObject();
        if (zkService.isExists(Constants.OGG_PROPERTIES_ROOT)) {
            oggConfJson = JSONObject.parseObject(new String(zkService.getData(Constants.OGG_PROPERTIES_ROOT), KeeperConstants.UTF8));
        }
        String host = map.get(KeeperConstants.HOST);
        String hosts = oggConfJson.getString(KeeperConstants.HOSTS);
        if (StringUtils.isNotBlank(hosts) && !hosts.contains(host)) {
            hosts = hosts + "," + host;
        } else {
            hosts = host;
        }
        oggConfJson.put(KeeperConstants.HOSTS, hosts);
        oggConfJson.put(KeeperConstants.PORT, map.get(KeeperConstants.PORT));
        oggConfJson.put(KeeperConstants.USER, map.get(KeeperConstants.USER));
        oggConfJson.put(KeeperConstants.OGG_PATH, map.get(KeeperConstants.OGG_PATH));
        oggConfJson.put(KeeperConstants.OGG_TOOL_PATH, map.get(KeeperConstants.OGG_TOOL_PATH));

        JSONObject dsOggConf = oggConfJson.getJSONObject(KeeperConstants.DS_OGG_CONF);
        if (dsOggConf != null) {
            JSONObject oggConf = new JSONObject();
            oggConf.put(KeeperConstants.REPLICAT_NAME, map.get(KeeperConstants.REPLICAT_NAME));
            oggConf.put(KeeperConstants.TRAIL_NAME, map.get(KeeperConstants.TRAIL_NAME));
            oggConf.put(KeeperConstants.NLS_LANG, map.get(KeeperConstants.NLS_LANG));
            oggConf.put(KeeperConstants.HOST, host);
            dsOggConf.put(dsName, oggConf);
        } else {
            dsOggConf = new JSONObject();
            JSONObject oggConf = new JSONObject();
            oggConf.put(KeeperConstants.REPLICAT_NAME, map.get(KeeperConstants.REPLICAT_NAME));
            oggConf.put(KeeperConstants.TRAIL_NAME, map.get(KeeperConstants.TRAIL_NAME));
            oggConf.put(KeeperConstants.NLS_LANG, map.get(KeeperConstants.NLS_LANG));
            oggConf.put(KeeperConstants.HOST, host);
            dsOggConf.put(dsName, oggConf);
        }
        oggConfJson.put(KeeperConstants.DS_OGG_CONF, dsOggConf);
        zkService.setData(Constants.OGG_PROPERTIES_ROOT,
                JsonFormatUtils.toPrettyFormat(oggConfJson.toJSONString()).getBytes(KeeperConstants.UTF8));
    }

    public String getOggTrailName() throws Exception {
        JSONObject oggConfJson = new JSONObject();
        if (zkService.isExists(Constants.OGG_PROPERTIES_ROOT)) {
            oggConfJson = JSONObject.parseObject(new String(zkService.getData(Constants.OGG_PROPERTIES_ROOT), KeeperConstants.UTF8));
        }
        JSONObject dsOggConf = oggConfJson.getJSONObject(KeeperConstants.DS_OGG_CONF);
        HashSet<String> trailNames = new HashSet<>();
        if (dsOggConf != null) {
            for (Object value : dsOggConf.values()) {
                String trailName = ((JSONObject) value).getString(KeeperConstants.TRAIL_NAME);
                trailNames.add(trailName);
            }
        }
        String newTrailName = null;
        for (int i = 97; i <= 122; i++) {
            for (int j = 97; j <= 122; j++) {
                newTrailName = "" + (char) i + (char) j;
                if (!trailNames.contains(newTrailName)) {
                    return newTrailName;
                }
            }
        }
        return null;
    }

    public JSONObject getCanalConf(String dsName) throws Exception {
        JSONObject canalConfJson = new JSONObject();
        if (zkService.isExists(Constants.CANAL_PROPERTIES_ROOT)) {
            canalConfJson = JSONObject.parseObject(new String(zkService.getData(Constants.CANAL_PROPERTIES_ROOT), KeeperConstants.UTF8));
        }
        JSONObject dsCanalConf = canalConfJson.getJSONObject(KeeperConstants.DS_CANAL_CONF);
        if (dsCanalConf != null) {
            JSONObject canalConf = dsCanalConf.getJSONObject(dsName);
            if (canalConf != null) {
                canalConfJson.put(KeeperConstants.HOST, canalConf.get(KeeperConstants.HOST));
                canalConfJson.put(KeeperConstants.CANAL_ADD, canalConf.get(KeeperConstants.CANAL_ADD));
            }
        }
        return canalConfJson;
    }

    public void setCanalConf(Map<String, String> map) throws Exception {
        String dsName = map.get("dsName");
        JSONObject canalConfJson = new JSONObject();
        if (zkService.isExists(Constants.CANAL_PROPERTIES_ROOT)) {
            canalConfJson = JSONObject.parseObject(new String(zkService.getData(Constants.CANAL_PROPERTIES_ROOT), KeeperConstants.UTF8));
        }
        String hosts = canalConfJson.getString(KeeperConstants.HOST);
        String host = map.get(KeeperConstants.HOST);
        if (StringUtils.isNotBlank(hosts) && !hosts.contains(host)) {
            hosts = hosts + "," + host;
        } else {
            hosts = host;
        }
        canalConfJson.put(KeeperConstants.HOSTS, hosts);
        canalConfJson.put(KeeperConstants.PORT, map.get(KeeperConstants.PORT));
        canalConfJson.put(KeeperConstants.USER, map.get(KeeperConstants.USER));
        canalConfJson.put(KeeperConstants.CANAL_PATH, map.get(KeeperConstants.CANAL_PATH));
        JSONObject dsCanalConf = canalConfJson.getJSONObject(KeeperConstants.DS_CANAL_CONF);
        if (dsCanalConf != null) {
            JSONObject canalConf = new JSONObject();
            canalConf.put(KeeperConstants.HOST, host);
            canalConf.put(KeeperConstants.CANAL_ADD, map.get(KeeperConstants.CANAL_ADD));
            dsCanalConf.put(dsName, canalConf);
        } else {
            dsCanalConf = new JSONObject();
            JSONObject canalConf = new JSONObject();
            canalConf.put(KeeperConstants.HOST, host);
            canalConf.put(KeeperConstants.CANAL_ADD, map.get(KeeperConstants.CANAL_ADD));
            dsCanalConf.put(dsName, canalConf);
        }
        canalConfJson.put(KeeperConstants.DS_CANAL_CONF, dsCanalConf);
        zkService.setData(Constants.CANAL_PROPERTIES_ROOT,
                JsonFormatUtils.toPrettyFormat(canalConfJson.toJSONString()).getBytes(KeeperConstants.UTF8));
    }

    public Boolean isAutoDeployOgg(String dsName) throws Exception {
        if (!zkService.isExists(Constants.OGG_PROPERTIES_ROOT)) {
            return false;
        }
        JSONObject oggConfJson = JSONObject.parseObject(new String(zkService.getData(Constants.OGG_PROPERTIES_ROOT), KeeperConstants.UTF8));
        JSONObject oggConf = oggConfJson.getJSONObject(KeeperConstants.DS_OGG_CONF).getJSONObject(dsName);
        if (oggConf != null) {
            return true;
        } else {
            return false;
        }
    }

    public Boolean isAutoDeployCanal(String dsName) throws Exception {
        if (!zkService.isExists(Constants.CANAL_PROPERTIES_ROOT)) {
            return false;
        }
        JSONObject canalConfJson = JSONObject.parseObject(new String(zkService.getData(Constants.CANAL_PROPERTIES_ROOT), KeeperConstants.UTF8));
        JSONObject canalConf = canalConfJson.getJSONObject(KeeperConstants.DS_CANAL_CONF).getJSONObject(dsName);
        if (canalConf != null) {
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

    /**
     * @param map
     * @return
     */
    public int addOracleSchema(HashMap<String, String> map) throws Exception {
        String dsName = map.get("dsName");
        String schemaName = map.get("schemaName");
        String tableNames = map.get("tableNames");
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
     * @param map
     * @return
     */
    public int addOracleTable(HashMap<String, String> map) throws Exception {
        String dsName = map.get("dsName");
        String schemaName = map.get("schemaName");
        String tableNames = map.get("tableNames");
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
        sb.append("; sh addLine.sh '-t newTable ");
        sb.append(" -sn ").append(schemaName);
        sb.append(" -tn ").append(tableNames);
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
        return 0;
    }

    /**
     * @param map
     * @return
     */
    public int deleteOracleSchema(HashMap<String, String> map) throws Exception {
        String dsName = map.get("dsName");
        String schemaName = map.get("schemaName");
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

    /**
     * @param map
     * @return
     */
    public int deleteOracleTable(HashMap<String, String> map) throws Exception {
        String dsName = map.get("dsName");
        String schemaName = map.get("schemaName");
        String tableNames = map.get("tableNames");
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

    public int addReplicat(String user, String host, String port, String replicatName, String trailName, String oggPath) {
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

        StringBuilder sb = new StringBuilder();
        sb.append("cd ").append(canalPath);
        sb.append(";sh addLine.sh '");
        sb.append(" -dn ").append(dsName);
        sb.append(" -zk ").append(zkString);
        sb.append(" -a ").append(canalAdd);
        sb.append(" -u ").append(canalUser);
        sb.append(" -p ").append(canalPass);
        sb.append("'");

        for (String ip : host.split(",")) {
            String result = SSHUtils.executeCommand(user, ip, Integer.parseInt(port), env.getProperty("pubKeyPath"), sb.toString(), true);
            if (result == null) {
                return MessageCode.SSH_CONF_ERROR;
            }
            if (StringUtils.isNotBlank(result)) {
                return MessageCode.DEPLOY_CANAL_ERROR;
            }
        }
        return 0;
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

        for (String ip : host.split(",")) {
            String result = SSHUtils.executeCommand(user, host, Integer.parseInt(port), env.getProperty("pubKeyPath"), sb.toString(), true);
            if (result == null) {
                return MessageCode.SSH_CONF_ERROR;
            }
            if (StringUtils.isNotBlank(result)) {
                return MessageCode.DEPLOY_CANAL_ERROR;
            }
        }
        return 0;
    }
}
