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
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.base.com.creditease.dbus.utils.RequestSender;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.ServiceNames;
import com.creditease.dbus.domain.model.DataSource;
import com.creditease.dbus.domain.model.StormTopology;
import com.creditease.dbus.utils.StormToplogyOpHelper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * Created by Administrator on 2018/8/5.
 */
@Service
public class FlowLineCheckService {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private RequestSender sender;
    @Autowired
    private DataSourceService dataSourceService;
    @Autowired
    private AutoDeployDataLineService autoDeployDataLineService;
    @Autowired
    private IZkService zkService;
    @Autowired
    private StormToplogyOpHelper stormTopoHelper;


    public ResultEntity checkFlowLine(Integer tableId) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/flow-line-check/check/{0}", "", tableId);
        return result.getBody();
    }

    public JSONObject checkDataLine(String dsName, String schemaName) throws Exception {
        JSONObject result = new JSONObject();
        DataSource dataSource = dataSourceService.getDataSourceByDsName(dsName);
        String dsType = dataSource.getDsType();
        //ogg,canal状态
        checkCanalOrOgg(dsName, result, dsType);
        //拓扑状态
        result.put("topologyStatus", checkTopology(dsName, dataSource.getDsType()));
        //主备
        result.put("masterSlaveDiffDate", checkMasterSlave(dataSource, schemaName));
        result.put("dsName", dsName);
        result.put("dsType", dataSource.getDsType());
        result.put("schemaName", schemaName);
        return result;
    }

    private Map<String, Object> checkMasterSlave(DataSource ds, String schemaName) throws Exception {
        ConcurrentHashMap<String, Object> map = new ConcurrentHashMap<>();
        Map<String, Object> masterParam = new ConcurrentHashMap();
        Map<String, Object> slaveParam = new ConcurrentHashMap();
        masterParam.put("dsType", ds.getDsType());
        slaveParam.put("dsType", ds.getDsType());
        if ("oracle".equals(ds.getDsType())) {
            masterParam.put("sql", "select  id, ds_name, schema_name, table_name, packet, create_time, update_time from DB_HEARTBEAT_MONITOR where schema_name = '" + schemaName + "'");
            slaveParam.put("sql", "select  id, ds_name, schema_name, table_name, packet, create_time, update_time from DB_HEARTBEAT_MONITOR where schema_name = '" + schemaName + "'");
        } else {
            masterParam.put("sql", "select  id, ds_name, schema_name, table_name, packet, create_time, update_time from db_heartbeat_monitor where schema_name = '" + schemaName + "'");
            slaveParam.put("sql", "select  id, ds_name, schema_name, table_name, packet, create_time, update_time from db_heartbeat_monitor where schema_name = '" + schemaName + "'");
        }
        masterParam.put("user", ds.getDbusUser());
        slaveParam.put("user", ds.getDbusUser());
        masterParam.put("password", ds.getDbusPwd());
        slaveParam.put("password", ds.getDbusPwd());
        masterParam.put("limit", 1);
        slaveParam.put("limit", 1);
        final CountDownLatch latch = new CountDownLatch(2);
        new Thread(() -> {
            try {
                masterParam.put("URL", ds.getMasterUrl());
                logger.info("get master time,param:{}", masterParam);
                List payload = sender.post(ServiceNames.KEEPER_SERVICE, "tables/execute-sql", masterParam).getBody().getPayload(List.class);
                if (payload != null) {
                    Map<String, Object> result = (Map<String, Object>) payload.get(0);
                    map.put("masterTime", result.get("CREATE_TIME"));
                } else {
                    map.put("masterTime", "error");
                }
                latch.countDown();
            } catch (Exception e) {
                logger.error("查询主库延时出错", e);
                map.put("masterTime", "error");
                latch.countDown();
            }
        }).start();

        new Thread(() -> {
            try {
                slaveParam.put("URL", ds.getSlaveUrl());
                logger.info("get slave time,param:{}", slaveParam);
                List payload = sender.post(ServiceNames.KEEPER_SERVICE, "tables/execute-sql", slaveParam).getBody().getPayload(List.class);
                if (payload != null) {
                    Map<String, Object> result = (Map<String, Object>) payload.get(0);
                    map.put("slaveTime", result.get("CREATE_TIME"));
                } else {
                    map.put("slaveTime", "error");
                }
                latch.countDown();
            } catch (Exception e) {
                logger.error("查询备库延时出错", e);
                map.put("slaveTime", "error");
                latch.countDown();
            }
        }).start();
        latch.await();
        logger.info("get master and slave time:{}", map);
        Object master = map.get("masterTime");
        Object slave = map.get("slaveTime");
        SimpleDateFormat sdf = null;
        if (!"error".equals(master) && !"error".equals(slave)) {
            String masterDate = master.toString();
            String slaveDate = slave.toString();
            if ("oracle".equals(ds.getDsType())) {
                //去除秒以下时间
                masterDate = masterDate.substring(0, masterDate.indexOf("."));
                slaveDate = slaveDate.substring(0, slaveDate.indexOf("."));
                sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
            } else {
                sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            }
            long masterTime = sdf.parse(masterDate).getTime();
            long slaveTime = sdf.parse(slaveDate).getTime();
            map.put("diffDate", diffDate(masterTime - slaveTime));
            map.put("diffTime", masterTime - slaveTime);
        } else {
            map.put("diffDate", "");
            map.put("diffTime", -1);
        }
        return map;
    }

    public void checkCanalOrOgg(String dsName, JSONObject result, String dsType) throws Exception {
        if ("oracle".equalsIgnoreCase(dsType)) {
            String oggReplicatStatus = autoDeployDataLineService.getOggReplicatStatus(dsName);
            if (StringUtils.isBlank(oggReplicatStatus)) {
                result.put("oggStatus", "ERROR,ogg config is null or get ogg status error");
            } else {
                result.put("oggStatus", oggReplicatStatus);
            }
        } else if ("mysql".equalsIgnoreCase(dsType)) {
            String pid = autoDeployDataLineService.getCanalPid(dsName);
            if (StringUtils.isBlank(pid)) {
                result.put("canalPid", "ERROR,canal config is null or get canal status error");
            } else {
                result.put("canalPid", "ok ,pid:" + pid.trim());
            }
        }
    }

    public static String diffDate(long diff) {
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
            ret.append(day + "天");
        }
        if (hour != 0) {
            ret.append(hour + "小时");
        }
        if (min != 0) {
            ret.append(min + "分钟");
        }
        if (sec != 0) {
            ret.append(sec + "秒");
        }
        return ret.length() == 0 ? "0秒" : ret.toString();
    }

    private JSONObject checkTopology(String dsName, String dsType) throws Exception {
        Map<String, StormTopology> runningTopologies = stormTopoHelper.getRunningTopologies();

        JSONObject topoInfo = new JSONObject();
        //if ("mysql".equalsIgnoreCase(dsType)) {
        //    if (runningTopologies.containsKey(dsName + "-mysql-extractor")) {
        //        topoInfo.put(dsName + "-mysql-extractor", "ok");
        //    } else {
        //        topoInfo.put(dsName + "-mysql-extractor", "not exist");
        //    }
        //}

        if ("oracle".equalsIgnoreCase(dsType) || "db2".equalsIgnoreCase(dsType) || "mysql".equalsIgnoreCase(dsType)) {
            if (runningTopologies.containsKey(dsName + "-dispatcher-appender")) {
                topoInfo.put(dsName + "-dispatcher-appender", "ok");
            } else {
                topoInfo.put(dsName + "-dispatcher-appender", "not exist");
                if (runningTopologies.containsKey(dsName + "-dispatcher")) {
                    topoInfo.put(dsName + "-dispatcher", "ok");
                    topoInfo.remove(dsName + "-dispatcher-appender");
                } else {
                    topoInfo.put(dsName + "-dispatcher", "not exist");
                }
                if (runningTopologies.containsKey(dsName + "-appender")) {
                    topoInfo.put(dsName + "-appender", "ok");
                } else {
                    topoInfo.put(dsName + "-appender", "not exist");
                }
            }
        }

        if (dsType.toLowerCase().indexOf("log") != -1) {
            if (runningTopologies.containsKey(dsName + "-log-processor")) {
                topoInfo.put(dsName + "-log-processor", "ok");
            } else {
                topoInfo.put(dsName + "-log-processor", "not exist");
            }
        }
        return topoInfo;
    }
}
