package com.creditease.dbus.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.utils.SSHUtils;
import com.creditease.dbus.utils.StormToplogyOpHelper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

/**
 * This is Description
 *
 * @author xiancangao
 * @date 2019/01/30
 */
@Service
public class TopologyService {
	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Autowired
	private IZkService zkService;
	@Autowired
	private Environment env;

	@Async
	public void batchRestartTopo(String jarPath, String topoType, ArrayList<String> dsNameList) throws Exception {
		Properties globalConf = zkService.getProperties(KeeperConstants.GLOBAL_CONF);
		String hostIp = globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOST);
		String port = globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_PORT);
		String stormBaseDir = globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_HOME_PATH);
		String stormSshUser = globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_SSH_USER);

		JSONArray topologies = getTopologySummary();
		HashMap<String, String> runningTopos = new HashMap<>();
		for (JSONObject topology : topologies.toJavaList(JSONObject.class)) {
			runningTopos.put(topology.getString("name"), topology.getString("id"));
		}
		logger.info("running topology list :{}", runningTopos);
		for (String dsName : dsNameList) {
			String topoId = runningTopos.get(dsName + "-" + topoType);
			if (StringUtils.isNotBlank(topoId)) {
				logger.info("find running topology {}", topoId);
				if (!StormToplogyOpHelper.inited) {
					StormToplogyOpHelper.init(zkService);
				}
				String killResult = StormToplogyOpHelper.killTopology(topoId, 10);
				if (StringUtils.isBlank(killResult) || !killResult.equals(StormToplogyOpHelper.OP_RESULT_SUCCESS)) {
					logger.error("Exception when kill topology {} ", topoId);
				}
				//等待3秒,防止没有kill掉
				Thread.sleep(3000);
				boolean killed = true;
				while (killed) {
					killed = false;
					topologies = getTopologySummary();
					for (JSONObject topology : topologies.toJavaList(JSONObject.class)) {
						String name = topology.getString("name");
						if (name.equals(dsName + "-" + topoType)) {
							Thread.sleep(3000);
							killed = true;
							break;
						}
					}
				}
				logger.info("kill running topology {} success", topoId);
			}
			String cmd = "cd " + stormBaseDir + "/" + KeeperConstants.STORM_JAR_DIR + ";";
			cmd += " ./dbus_startTopology.sh " + stormBaseDir + " " + topoType + " " + env.getProperty("zk.str");
			cmd += " " + dsName + " " + jarPath;
			logger.info("Topology Start Command:{}", cmd);
			String errorMsg = SSHUtils.executeCommand(stormSshUser, hostIp, Integer.parseInt(port), env.getProperty("pubKeyPath"), cmd, true);
			if (StringUtils.isNotBlank(errorMsg)) {
				logger.error("Exception when start topology {} ", dsName + topoType);
			}
			logger.info("start topology {} success", dsName + topoType);
		}
		logger.info("*************  batch restart topo end ************* ");
	}

	public JSONArray getTopologySummary() throws Exception {
		if (!StormToplogyOpHelper.inited) {
			StormToplogyOpHelper.init(zkService);
		}
		JSONObject topologySummary = JSON.parseObject(StormToplogyOpHelper.topologySummary());
		return topologySummary.getJSONArray("topologies");
	}
}
