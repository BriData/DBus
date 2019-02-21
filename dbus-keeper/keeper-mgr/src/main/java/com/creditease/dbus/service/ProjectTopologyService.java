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
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.text.MessageFormat;
import java.util.*;

import javax.websocket.Session;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.base.com.creditease.dbus.utils.RequestSender;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.constant.ServiceNames;
import com.creditease.dbus.domain.model.Project;
import com.creditease.dbus.domain.model.ProjectTopology;
import com.creditease.dbus.utils.StormToplogyOpHelper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

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
	private RestTemplate rest;
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

	public JSONArray getTopologySummary() throws Exception {
		if (!StormToplogyOpHelper.inited) {
			StormToplogyOpHelper.init(zkService);
		}
		JSONObject topologySummary = JSON.parseObject(StormToplogyOpHelper.topologySummary());
		return topologySummary.getJSONArray("topologies");
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
		logger.info("getTopoStatus originStatus:{},stormStatus:{},returnStatus:{}", originStatus, stormStatus, status);
		return status;
	}

	public String correctStatus(JSONArray topologies, String originStatus, String topologyName) {
		String stormStatus = "stopped";
		for (JSONObject topology : topologies.toJavaList(JSONObject.class)) {
			String name = topology.getString("name");
			if (StringUtils.equals(name, topologyName + "-router")
					&& StringUtils.equals(topology.getString("status"), "ACTIVE")) {
				stormStatus = "running";
			}
		}
		return getTopoStatus(originStatus, stormStatus);
	}

	public String correctStatus(String originStatus, String topologyName) throws Exception {
		JSONArray topologies = getTopologySummary();
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
			JSONArray topologies = getTopologySummary();
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

	/**
	 * storm.nimbus.host=vdbus-4
	 * zk.url=vdbus-7:2181
	 * storm.home.path=/app/dbus/apache-storm-1.0.2/
	 * <p>
	 * dbus.encode.plugins.jars.base.path=/app/dbus/apache-storm-1.0.2/dbus_encoder_plugins_jars
	 * dbus.jars.base.path=/app/dbus/apache-storm-1.0.2/dbus_jars
	 * dbus.router.jars.base.path=/app/dbus/apache-storm-1.0.2/dbus_router_jars
	 * dbus.kerberos.keytab.file.base.path=/app/dbus/apache-storm-1.0.2/dbus_keytab_files
	 *
	 * @param param
	 * @param session
	 * @return
	 * @throws Exception
	 */
	private ResultEntity start(Map<String, Object> param, Session session, SimpMessagingTemplate smt) throws Exception {
		if (session != null) session.getBasicRemote().sendText("请稍等,启动中...");
		if (smt != null) smt.convertAndSendToUser((String) param.get("uid"), "/log", "请稍等,启动中...");
		String topologyName = (String) param.get("topoName");
		String projectName = (String) param.get("projectName");
		String jarFilePath = (String) param.get("jarPath");
		String path = StringUtils.substringBeforeLast(jarFilePath, "/");
		String name = StringUtils.substringAfterLast(jarFilePath, "/");

		Properties props = zkService.getProperties(Constants.COMMON_ROOT + "/" + Constants.GLOBAL_PROPERTIES);
		String stormHomePath = props.getProperty("storm.home.path");
		if (StringUtils.endsWith(stormHomePath, "/"))
			stormHomePath = StringUtils.substringBeforeLast(stormHomePath, "/");

		String routerJarsBasePath = props.getProperty("dbus.router.jars.base.path");
		if (StringUtils.endsWith(routerJarsBasePath, "/"))
			routerJarsBasePath = StringUtils.substringBeforeLast(routerJarsBasePath, "/");

		String zkUrl = props.getProperty("zk.url");
		String user = props.getProperty("user");
		String stormNimbusHost = props.getProperty("storm.nimbus.host");
		String port = props.getProperty("storm.nimbus.port");

		// 0:routerJarsBasePath, 1:stormHomePath, 2:zk url, 3: topologyName, 4:jar path, 5 jar file name
		// 6:user name, 7:nimbus host, 8:ssh port, 9:project name
		String cmd = MessageFormat.format("ssh -p {8} {6}@{7} {0}/dbus_startTopology.sh {1} router {2} {3} {4}/ {5} {9}",
				routerJarsBasePath, stormHomePath, zkUrl, topologyName, path, name, user, stormNimbusHost, port, projectName);
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
		String user = props.getProperty("user");
		String stormNimbusHost = props.getProperty("storm.nimbus.host");
		String stormHomePath = props.getProperty("storm.home.path");
		if (StringUtils.endsWith(stormHomePath, "/"))
			stormHomePath = StringUtils.substringBeforeLast(stormHomePath, "/");

		String port = props.getProperty("storm.nimbus.port");

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

	private String httpGet(String serverUrl) throws Exception {
		StringBuilder responseBuilder = new StringBuilder();
		;
		BufferedReader reader = null;
		URL url = null;


		try {
			url = new URL(serverUrl);
			URLConnection conn = url.openConnection();
			conn.setDoOutput(true);
			conn.setConnectTimeout(1000 * 5);
			reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			String line = null;
			while ((line = reader.readLine()) != null) {
				responseBuilder.append(line).append("\n");
			}
		} catch (IOException e) {
			logger.error("http get error", e);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					logger.error("close reader error", e);
				}
			}
		}
		return responseBuilder.toString();
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
