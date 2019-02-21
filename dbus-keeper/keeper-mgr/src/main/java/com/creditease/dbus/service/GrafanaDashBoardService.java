package com.creditease.dbus.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.utils.HttpClientUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * This is Description
 *
 * @author xiancangao
 * @date 2019/01/29
 */
@Service
public class GrafanaDashBoardService {

	private org.slf4j.Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	private IZkService zkService;

	private static String host = null;
	private static String token = null;
	private static String api = null;
	private static boolean inited = false;

	public void init() throws Exception {
		if (!inited) {
			try {
				Properties properties = zkService.getProperties(Constants.COMMON_ROOT + "/" + Constants.GLOBAL_PROPERTIES);
				host = properties.getProperty("grafana_url_dbus");
				if (StringUtils.endsWith(host, "/")) {
					host = StringUtils.substringBeforeLast(host, "/");
				}
				token = properties.getProperty("grafanaToken");
				api = "/api/dashboards/db/";
				inited = true;
			} catch (Exception e) {
				logger.error("init grafana param error", e);
				throw e;
			}
		}
	}

	/**
	 * 删除grafana dashboard template table or topology regex
	 *
	 * @param dsName
	 * @param schemaName
	 * @param tableName
	 * @param projectName
	 * @throws Exception
	 */
	public void deleteDashboardTemplate(String dsName, String schemaName, String tableName, String projectName, String topoName) throws Exception {
		init();
		boolean deleteTopo = false;
		boolean deleteTable = false;
		if (StringUtils.isNotBlank(topoName)) {
			deleteTopo = true;
		}
		if (StringUtils.isNotBlank(dsName) && StringUtils.isNotBlank(schemaName) && StringUtils.isNotBlank(tableName)) {
			deleteTable = true;
		}
		String url = host + api + projectName;
		try {
			List<Object> ret = HttpClientUtils.send(url, "GET", "", token);
			if ((int) ret.get(0) == 200) {
				boolean isFind = false;
				String strJson = (String) ret.get(1);
				JSONObject json = JSONObject.parseObject(strJson);
				JSONObject dashboard = json.getJSONObject("dashboard");
				JSONObject templating = dashboard.getJSONObject("templating");
				JSONArray list = templating.getJSONArray("list");
				if (list != null && list.size() > 0) {
					for (int i = 0; i < list.size(); i++) {
						JSONObject item = list.getJSONObject(i);
						String regex = item.getString("regex");
						if (deleteTable && StringUtils.equalsIgnoreCase(item.getString("name"), "table")) {
							String wkTable = StringUtils.join(new String[]{dsName, schemaName, tableName}, ".");
							StringBuilder regexSb = new StringBuilder();
							String[] split = regex.split("\\|");
							for (int j = 0; j < split.length; j++) {
								if (StringUtils.equals(split[j], wkTable)) {
									split[j] = null;
								}
							}
							for (String str : split) {
								if (StringUtils.isNotBlank(str) && !StringUtils.equalsIgnoreCase(str, "none")) {
									regexSb.append(str);
									regexSb.append("|");
								}
							}
							if (StringUtils.endsWith(regexSb.toString(), "|")) {
								regex = StringUtils.substringBeforeLast(regexSb.toString(), "|");
							} else {
								regex = regexSb.toString();
							}
							if (StringUtils.isBlank(regex)) {
								item.put("regex", "none");
							} else {
								item.put("regex", regex);
							}

							isFind = true;
						}

						if (deleteTopo && StringUtils.equalsIgnoreCase(item.getString("name"), "rounter_type")) {
							String wkType = "ROUTER_TYPE_" + topoName;
							StringBuilder regexSb = new StringBuilder();
							String[] split = regex.split("\\|");
							for (int j = 0; j < split.length; j++) {
								if (StringUtils.equals(split[j], wkType)) {
									split[j] = null;
								}
							}
							for (String str : split) {
								if (StringUtils.isNotBlank(str) && !StringUtils.equalsIgnoreCase(str, "none")) {
									regexSb.append(str);
									regexSb.append("|");
								}
							}
							if (StringUtils.endsWith(regexSb.toString(), "|")) {
								regex = StringUtils.substringBeforeLast(regexSb.toString(), "|");
							} else {
								regex = regexSb.toString();
							}
							if (StringUtils.isBlank(regex)) {
								item.put("regex", "none");
							} else {
								item.put("regex", regex);
							}
							isFind = true;
						}
					}
					if (isFind) {
						updateDashboardTemplate(json);
					}
				}
			} else if (((int) ret.get(0) == -1)) {
				logger.error("call url:{} fail", url);
			} else {
				logger.warn("call url:{} response msg:{}", url, ret.get(1));
			}

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	private void updateDashboardTemplate(JSONObject json) {
		String url = host + api;
		String param = json.toJSONString();
		logger.info("update dashboard param: {}", param);
		List<Object> ret = HttpClientUtils.send(url, "POST", param, token);
		if ((int) ret.get(0) == 200) {
			logger.info("update dashboard success, {}", (String) ret.get(1));
		} else if (((int) ret.get(0) == -1)) {
			logger.error("call url:{} fail", url);
		} else {
			logger.warn("call url:{} response msg:{}", url, (String) ret.get(1));
		}
	}


	/**
	 * 批量删除grafana dashboard template table regex
	 *
	 * @param topoTableMap
	 */
	public void deleteDashboardTemplate(Map<String, List<Map<String, Object>>> topoTableMap) throws
			Exception {
		init();
		for (Map.Entry<String, List<Map<String, Object>>> entry : topoTableMap.entrySet()) {
			String projectName = entry.getKey();
			List<Map<String, Object>> topoTables = entry.getValue();
			try {
				String url = host + api + projectName;
				List<Object> ret = HttpClientUtils.send(url, "GET", "", token);
				if ((int) ret.get(0) == 200) {
					String strJson = (String) ret.get(1);
					JSONObject json = JSONObject.parseObject(strJson);
					JSONObject dashboard = json.getJSONObject("dashboard");
					JSONObject templating = dashboard.getJSONObject("templating");
					JSONArray list = templating.getJSONArray("list");
					if (list != null && list.size() > 0) {

						boolean isFind = false;
						for (int i = 0; i < list.size(); i++) {
							JSONObject item = list.getJSONObject(i);
							if (StringUtils.equalsIgnoreCase(item.getString("name"), "table")) {
								String regex = item.getString("regex");
								String[] split = regex.split("\\|");
								for (Map<String, Object> topoTable : topoTables) {
									String wkTable = StringUtils.join(new String[]{(String) topoTable.get("ds_name"),
											(String) topoTable.get("schema_name"), (String) topoTable.get("table_name")}, ".");
									for (int j = 0; j < split.length; j++) {
										if (StringUtils.equals(split[j], wkTable)) {
											split[j] = null;
										}
									}
								}
								StringBuilder regexSb = new StringBuilder();
								for (String str : split) {
									if (StringUtils.isNotBlank(str) && !StringUtils.equalsIgnoreCase(str, "none")) {
										regexSb.append(str);
										regexSb.append("|");
									}
								}
								if (StringUtils.endsWith(regexSb.toString(), "|")) {
									regex = StringUtils.substringBeforeLast(regexSb.toString(), "|");
								} else {
									regex = regexSb.toString();
								}
								if (StringUtils.isBlank(regex)) {
									item.put("regex", "none");
								} else {
									item.put("regex", regex);
								}
								isFind = true;
							}
						}
						if (isFind) {
							updateDashboardTemplate(json);
						}
					}
				} else if (((int) ret.get(0) == -1)) {
					logger.error("call url:{} fail", url);
				} else {
					logger.warn("call url:{} response msg:{}", url, ret.get(1));
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
	}

	public void addDashboardTemplateTablesAndTopos
			(Map<String, List<Map<String, Object>>> topoTableMap, HashSet<String> topoNames,
			 ResultEntity resultEntity) throws Exception {
		init();
		Properties props = zkService.getProperties(Constants.COMMON_ROOT + "/" + Constants.GLOBAL_PROPERTIES);
		String host = props.getProperty("grafana_url_dbus");
		String token = props.getProperty("grafanaToken");
		String api = "/api/dashboards/db/";
		if (StringUtils.endsWith(host, "/")) {
			host = StringUtils.substringBeforeLast(host, "/");
		}
		try {
			for (Map.Entry<String, List<Map<String, Object>>> entry : topoTableMap.entrySet()) {
				String projectName = entry.getKey();
				List<Map<String, Object>> topoTables = entry.getValue();
				String url = host + api + projectName;

				List<Object> ret = HttpClientUtils.send(url, "GET", "", token);
				//导入模板初始化
				if ((int) ret.get(0) == 404) {
					url = host + api;
					String param = "{\"meta\":{\"type\":\"db\",\"canSave\":true,\"canEdit\":true,\"canStar\":true,\"slug\":\"dbus_placeholder_slug\",\"expires\":\"0001-01-01T00:00:00Z\",\"created\":\"2018-10-10T14:36:55+08:00\",\"updated\":\"2018-10-17T11:36:46+08:00\",\"updatedBy\":\"admin\",\"createdBy\":\"admin\",\"version\":30},\"dashboard\":{\"annotations\":{\"list\":[]},\"editable\":true,\"gnetId\":null,\"graphTooltip\":0,\"hideControls\":false,\"links\":[],\"refresh\":false,\"rows\":[{\"collapse\":false,\"height\":\"250px\",\"panels\":[{\"aliasColors\":{},\"bars\":false,\"datasource\":\"inDB\",\"fill\":1,\"id\":1,\"legend\":{\"avg\":false,\"current\":false,\"max\":false,\"min\":false,\"show\":true,\"total\":false,\"values\":false},\"lines\":true,\"linewidth\":1,\"links\":[],\"nullPointMode\":\"null\",\"percentage\":false,\"pointradius\":5,\"points\":false,\"renderer\":\"flot\",\"seriesOverrides\":[],\"span\":12,\"stack\":false,\"steppedLine\":false,\"targets\":[{\"alias\":\"分发器计数\",\"dsType\":\"influxdb\",\"groupBy\":[],\"measurement\":\"dbus_statistic\",\"policy\":\"default\",\"query\":\"SELECT \\\"count\\\" FROM \\\"measurement\\\" WHERE \\\"table\\\" =~ /^$table$/ AND \\\"type\\\" = 'DISPATCH_TYPE' AND $timeFilter\",\"rawQuery\":false,\"refId\":\"A\",\"resultFormat\":\"time_series\",\"select\":[[{\"params\":[\"count\"],\"type\":\"field\"}]],\"tags\":[{\"key\":\"table\",\"operator\":\"=~\",\"value\":\"/^$table$/\"},{\"condition\":\"AND\",\"key\":\"type\",\"operator\":\"=\",\"value\":\"DISPATCH_TYPE\"}]},{\"alias\":\"增量计数\",\"dsType\":\"influxdb\",\"groupBy\":[],\"measurement\":\"dbus_statistic\",\"policy\":\"default\",\"query\":\"SELECT \\\"count\\\" FROM \\\"measurement\\\" WHERE \\\"table\\\" =~ /^$table$/ AND \\\"type\\\" = 'APPENDER_TYPE' AND $timeFilter\",\"rawQuery\":false,\"refId\":\"B\",\"resultFormat\":\"time_series\",\"select\":[[{\"params\":[\"count\"],\"type\":\"field\"}]],\"tags\":[{\"key\":\"table\",\"operator\":\"=~\",\"value\":\"/^$table$/\"},{\"condition\":\"AND\",\"key\":\"type\",\"operator\":\"=\",\"value\":\"APPENDER_TYPE\"}]},{\"alias\":\"router计数\",\"dsType\":\"influxdb\",\"groupBy\":[],\"measurement\":\"dbus_statistic\",\"policy\":\"default\",\"query\":\"SELECT \\\"count\\\" FROM \\\"measurement\\\" WHERE \\\"table\\\" =~ /^$table$/ AND \\\"type\\\" = 'ROUTER_TYPE_testRouterSec' AND $timeFilter\",\"rawQuery\":false,\"refId\":\"C\",\"resultFormat\":\"time_series\",\"select\":[[{\"params\":[\"count\"],\"type\":\"field\"}]],\"tags\":[{\"key\":\"table\",\"operator\":\"=~\",\"value\":\"/^$table$/\"},{\"condition\":\"AND\",\"key\":\"type\",\"operator\":\"=~\",\"value\":\"/^$rounter_type$/\"}]}],\"thresholds\":[],\"timeFrom\":null,\"timeShift\":null,\"title\":\"表统计计数\",\"tooltip\":{\"shared\":true,\"sort\":0,\"value_type\":\"individual\"},\"type\":\"graph\",\"xaxis\":{\"mode\":\"time\",\"name\":null,\"show\":true,\"values\":[]},\"yaxes\":[{\"format\":\"short\",\"label\":null,\"logBase\":1,\"max\":null,\"min\":null,\"show\":true},{\"format\":\"short\",\"label\":null,\"logBase\":1,\"max\":null,\"min\":null,\"show\":true}]}],\"repeat\":null,\"repeatIteration\":null,\"repeatRowId\":null,\"showTitle\":false,\"title\":\"Dashboard Row\",\"titleSize\":\"h6\"},{\"collapse\":false,\"height\":250,\"panels\":[{\"aliasColors\":{},\"bars\":false,\"datasource\":\"inDB\",\"fill\":1,\"id\":2,\"legend\":{\"avg\":false,\"current\":false,\"max\":false,\"min\":false,\"show\":true,\"total\":false,\"values\":false},\"lines\":true,\"linewidth\":1,\"links\":[],\"nullPointMode\":\"null\",\"percentage\":false,\"pointradius\":5,\"points\":false,\"renderer\":\"flot\",\"seriesOverrides\":[],\"span\":12,\"stack\":false,\"steppedLine\":false,\"targets\":[{\"alias\":\"分发器延时\",\"dsType\":\"influxdb\",\"groupBy\":[],\"measurement\":\"dbus_statistic\",\"policy\":\"default\",\"refId\":\"A\",\"resultFormat\":\"time_series\",\"select\":[[{\"params\":[\"latency\"],\"type\":\"field\"}]],\"tags\":[{\"key\":\"table\",\"operator\":\"=~\",\"value\":\"/^$table$/\"},{\"condition\":\"AND\",\"key\":\"type\",\"operator\":\"=\",\"value\":\"DISPATCH_TYPE\"}]},{\"alias\":\"增量延时\",\"dsType\":\"influxdb\",\"groupBy\":[],\"measurement\":\"dbus_statistic\",\"policy\":\"default\",\"query\":\"SELECT \\\"latency\\\" FROM \\\"dbus_statistic\\\" WHERE \\\"table\\\" =~ /^$table$/ AND \\\"type\\\" = 'DISPATCH_TYPE' AND $timeFilter\",\"rawQuery\":false,\"refId\":\"B\",\"resultFormat\":\"time_series\",\"select\":[[{\"params\":[\"latency\"],\"type\":\"field\"}]],\"tags\":[{\"key\":\"table\",\"operator\":\"=~\",\"value\":\"/^$table$/\"},{\"condition\":\"AND\",\"key\":\"type\",\"operator\":\"=\",\"value\":\"APPENDER_TYPE\"}]},{\"alias\":\"router延时\",\"dsType\":\"influxdb\",\"groupBy\":[],\"measurement\":\"dbus_statistic\",\"policy\":\"default\",\"query\":\"SELECT \\\"latency\\\" FROM \\\"dbus_statistic\\\" WHERE \\\"table\\\" =~ /^$table$/ AND \\\"type\\\" = 'DISPATCH_TYPE' AND $timeFilter\",\"rawQuery\":false,\"refId\":\"C\",\"resultFormat\":\"time_series\",\"select\":[[{\"params\":[\"latency\"],\"type\":\"field\"}]],\"tags\":[{\"key\":\"table\",\"operator\":\"=~\",\"value\":\"/^$table$/\"},{\"condition\":\"AND\",\"key\":\"type\",\"operator\":\"=~\",\"value\":\"/^$rounter_type$/\"}]}],\"thresholds\":[],\"timeFrom\":null,\"timeShift\":null,\"title\":\"延时\",\"tooltip\":{\"shared\":true,\"sort\":0,\"value_type\":\"individual\"},\"type\":\"graph\",\"xaxis\":{\"mode\":\"time\",\"name\":null,\"show\":true,\"values\":[]},\"yaxes\":[{\"format\":\"short\",\"label\":null,\"logBase\":1,\"max\":null,\"min\":null,\"show\":true},{\"format\":\"short\",\"label\":null,\"logBase\":1,\"max\":null,\"min\":null,\"show\":true}]}],\"repeat\":null,\"repeatIteration\":null,\"repeatRowId\":null,\"showTitle\":false,\"title\":\"Dashboard Row\",\"titleSize\":\"h6\"}],\"schemaVersion\":14,\"style\":\"dark\",\"tags\":[],\"templating\":{\"list\":[{\"allValue\":null,\"datasource\":\"inDB\",\"hide\":0,\"includeAll\":false,\"label\":null,\"multi\":false,\"name\":\"table\",\"options\":[],\"query\":\"SHOW TAG VALUES WITH KEY = \\\"table\\\"\",\"refresh\":1,\"regex\":\"dbus_placeholder_table_regex\",\"sort\":0,\"tagValuesQuery\":\"\",\"tags\":[],\"tagsQuery\":\"\",\"type\":\"query\",\"useTags\":false},{\"allValue\":null,\"datasource\":\"inDB\",\"hide\":0,\"includeAll\":false,\"label\":null,\"multi\":false,\"name\":\"rounter_type\",\"options\":[],\"query\":\"SHOW TAG VALUES WITH KEY = \\\"type\\\"\",\"refresh\":1,\"regex\":\"dbus_placeholder_type_regex\",\"sort\":0,\"tagValuesQuery\":\"\",\"tags\":[],\"tagsQuery\":\"\",\"type\":\"query\",\"useTags\":false}]},\"time\":{\"from\":\"now-30m\",\"to\":\"now\"},\"timepicker\":{\"refresh_intervals\":[\"5s\",\"10s\",\"30s\",\"1m\",\"5m\",\"15m\",\"30m\",\"1h\",\"2h\",\"1d\"],\"time_options\":[\"5m\",\"15m\",\"1h\",\"6h\",\"12h\",\"24h\",\"2d\",\"7d\",\"30d\"]},\"timezone\":\"browser\",\"title\":\"dbus_placeholder_slug\",\"version\":30}}";
					param = StringUtils.replace(param, "dbus_placeholder_slug", projectName);
					param = StringUtils.replace(param, "dbus_placeholder_table_regex", "none");
					param = StringUtils.replace(param, "dbus_placeholder_type_regex", "none");
					logger.info("create dashboard param: {}", param);
					ret = HttpClientUtils.send(url, "POST", param, token);
					if ((int) ret.get(0) == 200) {
						logger.info("create dashboard success, {}", (String) ret.get(1));
					} else if (((int) ret.get(0) == -1)) {
						logger.error("call url:{} fail", url);
						resultEntity.setStatus(MessageCode.CREATE_DASHBOARD_ERROR);
						return;
					} else {
						logger.warn("call url:{} response msg:{}", url, (String) ret.get(1));
					}
					ret = HttpClientUtils.send(host + api + projectName, "GET", "", token);
				}

				//添加router和table
				if ((int) ret.get(0) == 200) {
					boolean isFind = false;
					String strJson = (String) ret.get(1);
					JSONObject json = JSONObject.parseObject(strJson);
					JSONObject dashboard = json.getJSONObject("dashboard");
					JSONObject templating = dashboard.getJSONObject("templating");
					JSONArray list = templating.getJSONArray("list");
					if (list != null && list.size() > 0) {
						for (int i = 0; i < list.size(); i++) {
							JSONObject item = list.getJSONObject(i);
							String name = item.getString("name");
							String regex = item.getString("regex");
							StringBuilder regexSb = new StringBuilder();
							for (String str : regex.split("\\|")) {
								if (StringUtils.isNotBlank(str) &&
										!StringUtils.equalsIgnoreCase(str, "none")) {
									regexSb.append(str);
									regexSb.append("|");
								}
							}
							if (StringUtils.endsWith(regexSb.toString(), "|")) {
								regex = StringUtils.substringBeforeLast(regexSb.toString(), "|");
							} else {
								regex = regexSb.toString();
							}
							if (StringUtils.equalsIgnoreCase(name, "table")) {
								for (Map<String, Object> topoTable : topoTables) {
									String wkTable = StringUtils.join(new String[]{(String) topoTable.get("ds_name"),
											(String) topoTable.get("schema_name"), (String) topoTable.get("table_name")}, ".");
									boolean contains = false;
									for (String str : regex.split("\\|")) {
										if (StringUtils.equalsIgnoreCase(str, wkTable)) {
											contains = true;
										}
									}
									if (!contains) {
										if (StringUtils.isNotBlank(regex)) {
											regex = StringUtils.join(new String[]{regex, wkTable}, "|");
										} else {
											regex = wkTable;
										}
									}
								}
								item.put("regex", regex);
								isFind = true;
							}
							if (StringUtils.equalsIgnoreCase(name, "rounter_type")) {
								for (String topoName : topoNames) {
									String wkType = "ROUTER_TYPE_" + topoName;
									boolean contains = false;
									for (String str : regex.split("\\|")) {
										if (StringUtils.equalsIgnoreCase(str, wkType)) {
											contains = true;
										}
									}
									if (!contains) {
										if (StringUtils.isNotBlank(regex)) {
											regex = StringUtils.join(new String[]{regex, wkType}, "|");
										} else {
											regex = wkType;
										}
									}
								}
								item.put("regex", regex);
								isFind = true;
							}
						}
						if (isFind) {
							updateDashboardTemplate(json);
						}
					}
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	/**
	 * 删除grafana dashboard
	 *
	 * @throws Exception
	 */
	public void deleteDashboard(String projectName) throws Exception {
		init();
		try {
			String url = host + api + projectName;
			List<Object> ret = HttpClientUtils.send(url, "GET", "", token);
			if ((int) ret.get(0) == 200) {
				ret = HttpClientUtils.send(url, "DELETE", "", token);
				if ((int) ret.get(0) == 200) {
					logger.info("delete dashboard success, {}", ret.get(1));
				} else if (((int) ret.get(0) == -1)) {
					logger.error("call url:{} fail", url);
				} else {
					logger.warn("call url:{} response msg:{}", url, ret.get(1));
				}
			} else if (((int) ret.get(0) == -1)) {
				logger.error("call url:{} fail", url);
			} else {
				logger.warn("call url:{} response msg:{}", url, ret.get(1));
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}

	}

}
