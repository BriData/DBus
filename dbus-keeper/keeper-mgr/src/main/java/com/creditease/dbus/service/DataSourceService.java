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

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.base.com.creditease.dbus.utils.RequestSender;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.constant.ServiceNames;
import com.creditease.dbus.domain.model.DataSource;
import com.creditease.dbus.domain.model.DataTable;
import com.creditease.dbus.utils.DelZookeeperNodesTemplate;
import com.creditease.dbus.utils.OrderedProperties;
import com.creditease.dbus.utils.SSHUtils;
import com.creditease.dbus.utils.StormToplogyOpHelper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.github.pagehelper.PageInfo;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.net.URLDecoder;
import java.util.*;

/**
 * User: 王少楠
 * Date: 2018-05-08
 * Time: 上午11:38
 */
@Service
public class DataSourceService {
    private Logger logger = LoggerFactory.getLogger(getClass());
    @Autowired
    private RequestSender sender;
    @Autowired
    private IZkService zkService;
    @Autowired
    private Environment env;
    @Autowired
    private ZkConfService zkConfService;
    @Autowired
    private TableService tableService;
    @Autowired
    private ToolSetService toolSetService;
    @Autowired
    private AutoDeployDataLineService autoDeployDataLineService;

    private static final String KEEPER_SERVICE = ServiceNames.KEEPER_SERVICE;

    private final String OBEJCT_COLUMN = "--------";//查询的dbusData不是table，是存储过程时对应的column位置的值

    /**
     * datasource首页的搜索
     *
     * @param queryString param:dsName,if ds=null get all
     */
    public ResultEntity search(String queryString) throws Exception {
        ResponseEntity<ResultEntity> result;
        if (queryString == null || queryString.isEmpty()) {
            result = sender.get(KEEPER_SERVICE, "/datasource/search");
        } else {
            queryString = URLDecoder.decode(queryString, "UTF-8");
            result = sender.get(KEEPER_SERVICE, "/datasource/search", queryString);
        }
        ResultEntity body = result.getBody();
        PageInfo<Map<String, Object>> dataSourceList = body.getPayload(new TypeReference<PageInfo<Map<String, Object>>>() {
        });
        for (Map<String, Object> ds : dataSourceList.getList()) {
            String dsName = (String) ds.get("name");
            if (ds.get("type").equals("mysql")) {
                JSONObject canalConf = autoDeployDataLineService.getCanalConf(dsName);
                ds.put("oggOrCanalHost",canalConf.getString(KeeperConstants.HOST));
                ds.put("oggOrCanalPath",canalConf.getString(KeeperConstants.CANAL_PATH));
            }
            if (ds.get("type").equals("oracle")) {
                JSONObject oggConf = autoDeployDataLineService.getOggConf(dsName);
                ds.put("oggOrCanalHost",oggConf.getString(KeeperConstants.HOST));
                ds.put("oggOrCanalPath",oggConf.getString(KeeperConstants.OGG_PATH));
                ds.put("oggReplicatName",oggConf.getString(KeeperConstants.REPLICAT_NAME));
                ds.put("oggTrailName",oggConf.getString(KeeperConstants.TRAIL_NAME));
            }
        }
        body.setPayload(dataSourceList);
        return body;
    }

    public ResultEntity getById(Integer id) {
        ResponseEntity<ResultEntity> result = sender.get(KEEPER_SERVICE, "/datasource/{id}", id);
        return result.getBody();
    }

    public ResultEntity insertOne(DataSource dataSource) {
        ResponseEntity<ResultEntity> result = sender.post(KEEPER_SERVICE, "/datasource", dataSource);
        return result.getBody();
    }

    public ResultEntity update(DataSource dataSource) throws Exception {
        if (dataSource.getStatus().equals("inactive")) {
            List<DataTable> tables = sender.get(KEEPER_SERVICE, "/tables/findActiveTablesByDsId/{0}", dataSource.getId()).getBody().getPayload(new TypeReference<List<DataTable>>() {
            });
            if (tables != null && tables.size() > 0) {
                return new ResultEntity(15013, "请先停止该数据源下所有表,再inactive该数据源");
            }
        }
        ResponseEntity<ResultEntity> result = sender.post(KEEPER_SERVICE, "/datasource/update", dataSource);
        return result.getBody();
    }

    public int countActiveTables(Integer id) {
        //是否还有项目在使用
        Integer count = sender.get(KEEPER_SERVICE, "/projectTable/count-by-ds-id/{id}", id).getBody().getPayload(Integer.class);
        //是否还有running的表
        List<DataTable> tables = sender.get(KEEPER_SERVICE, "/tables/findActiveTablesByDsId/{0}", id)
                .getBody().getPayload(new TypeReference<List<DataTable>>() {
                });
        return count + tables.size();
    }

    public ResultEntity delete(Integer id) throws Exception {
        DataSource dataSource = this.getById(id).getPayload(DataSource.class);
        String dsName = dataSource.getDsName();
        //删除zk节点
        try {
            delDsZkConf(DelZookeeperNodesTemplate.ZK_CLEAR_NODES_PATHS, dsName);
            delDsZkConf(DelZookeeperNodesTemplate.ZK_CLEAR_NODES_PATHS_OF_DSNAME_TO_UPPERCASE, dsName.toUpperCase());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        //级联删除相关表数据
        ResultEntity body = sender.get(KEEPER_SERVICE, "/datasource/delete/{id}", id).getBody();
        if (body.getStatus() != 0) {
            return body;
        }
        //自动删除ogg或者canal
        ResultEntity resultEntity = new ResultEntity();
        resultEntity.setStatus(autoDeleteOggCanalLine(dataSource));
        return resultEntity;
    }

    public ResultEntity getDataSourceByName(String name) {
        ResponseEntity<ResultEntity> result = sender.get(KEEPER_SERVICE, "/datasource/getDataSourceByName", name);
        return result.getBody();
    }

    public ResultEntity searchFromSource(Integer dsId) {
        ResultEntity result = getById(dsId);
        if (!result.success()) {
            return result;
        }
        //根据dsId获取需要的参数信息
        DataSource dataSource = result.getPayload(new TypeReference<DataSource>() {
        });
        Map<String, Object> params = new HashedMap(5);
        params.put("dsId", dsId);
        params.put("dsType", dataSource.getDsType());
        params.put("URL", dataSource.getMasterUrl());
        params.put("user", dataSource.getDbusUser());
        params.put("password", dataSource.getDbusPwd());
        //调用接口查询
        ResponseEntity<ResultEntity> responseEntity = sender.post(KEEPER_SERVICE, "/datasource/searchFromSource", params);
        if (!responseEntity.getStatusCode().is2xxSuccessful() || !responseEntity.getBody().success()) {
            return responseEntity.getBody();
        }
        List<String> resultList = responseEntity.getBody().getPayload(new TypeReference<List<String>>() {
        });
        //将结果数据格式化成前端需要的数据
        List<String> structureList = new ArrayList<>();
        HashSet<String> tableNames = new HashSet<>();
        JSONObject tableMsg = null;
        for (int i = 0; i < resultList.size(); i++) {
            String[] columnInfo = resultList.get(i).split("/");// "tablename/columnname, type"
            if (!tableNames.contains(columnInfo[0])) {
                if (tableMsg != null) {
                    structureList.add(tableMsg.toJSONString());
                }
                tableMsg = new JSONObject();
                if (StringUtils.equals(columnInfo[1], OBEJCT_COLUMN)) {
                    tableMsg.put("type", "存储过程");
                    tableMsg.put("name", columnInfo[0]);
                    tableMsg.put("exist", "是");
                    tableMsg.put("column", columnInfo[1]);
                } else {
                    tableMsg.put("type", "表");
                    tableMsg.put("name", columnInfo[0]);
                    tableMsg.put("exist", "是");
                    tableMsg.put("column", columnInfo[1]);
                }
                tableNames.add(columnInfo[0]);
            } else {
                String column = tableMsg.getString("column");
                tableMsg.put("column", column + "   " + columnInfo[1]);
            }
        }
        structureList.add(tableMsg.toJSONString());
        /*
        String tableName ="";//当前table的name
        StringBuffer columnsInfo =new StringBuffer();//某个table中需要添加的column信息
        boolean tableTail = false; //标识：最后一次添加表或存储过程
        for(int i=0;i<resultList.size();i++){
            String[] columnInfo = resultList.get(i).split("/");// "tablename/columnname, type"
            if(i==0){
                tableName = columnInfo[0];//表名，起始进行初始化
            }
            if(StringUtils.equals(tableName,columnInfo[0])){//当前列表名与之前一致，添加到当前table信息中
                columnsInfo.append(" ").append(columnInfo[1]);
            }else {//当前列表名与之前不一致，说明该表信息添加完毕
                //初始化表的基本信息，如果是存储过程，后面直接更新"type"的值
                JSONObject tableMsg = new JSONObject();
                tableMsg.put("type","表");
                tableMsg.put("name",tableName);
                tableMsg.put("exist","是");
                tableMsg.put("column",columnsInfo.toString());
                //检查存储过程
                if(StringUtils.equals(columnInfo[1],OBEJCT_COLUMN)){
                    if(!tableTail){
                        structureList.add(tableMsg.toJSONString());
                        tableTail=true;
                    }else {
                        tableMsg.put("type","存储过程");
                        structureList.add(tableMsg.toJSONString());
                    }
                }else {
                    structureList.add(tableMsg.toJSONString());
                }
                //重新记录表名和column信息
                tableName=columnInfo[0];
                columnsInfo.setLength(0);//清空stringbuffer
                columnsInfo.append(columnInfo[1]);
            }
        }
        //将最后一个表的内容添加到结果集
        JSONObject tableMsg = new JSONObject();
        tableMsg.put("type","表");
        tableMsg.put("name",tableName);
        tableMsg.put("exist","是");
        tableMsg.put("column",columnsInfo.toString());
        if(DbusDatasourceType.parse(dataSource.getDsType()) == DbusDatasourceType.ORACLE){
            tableMsg.put("type","存储过程");
            structureList.add(tableMsg.toJSONString());
        }else if(DbusDatasourceType.parse(dataSource.getDsType()) == DbusDatasourceType.MYSQL){
            structureList.add(tableMsg.toJSONString());
        }*/

        //result.setPayload(structureList);
        result = new ResultEntity();
        result.setPayload(structureList);
        return result;
    }

    public ResultEntity validateDataSources(Map<String, Object> map) {
        ResponseEntity<ResultEntity> result = sender.post(KEEPER_SERVICE, "/datasource/validate", map);
        return result.getBody();
    }

    public ResultEntity modifyDataSourceStatus(Long id, String status) {
        ResponseEntity<ResultEntity> result = sender.get(KEEPER_SERVICE, "/datasource/{id}/{status}", id, status);
        return result.getBody();
    }

    public ResultEntity getDSNames() {
        ResponseEntity<ResultEntity> result = sender.get(KEEPER_SERVICE, "/datasource/getDSNames");
        return result.getBody();
    }

    public String startTopology(String dsName, String jarPath, String jarName, String topologyType) throws Exception {
        Properties globalConf = zkService.getProperties(KeeperConstants.GLOBAL_CONF);

        String hostIp = globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOST);
        String port = globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_PORT);
        String stormBaseDir = globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_HOME_PATH);
        String stormSshUser = globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_SSH_USER);

        String cmd = "cd " + stormBaseDir + "/" + KeeperConstants.STORM_JAR_DIR + ";";
        cmd += " ./dbus_startTopology.sh " + stormBaseDir + " " + topologyType + " " + env.getProperty("zk.str");
        cmd += " " + dsName + " " + jarPath + " " + jarName;
        // String sshCmd = "ssh -p " + port + " " + stormSshUser + "@" + hostIp + " " + "'" + cmd + "'";

        logger.info("Topology Start Command:{}", cmd);
        return SSHUtils.executeCommand(stormSshUser, hostIp, Integer.parseInt(port), env.getProperty("pubKeyPath"), cmd, null);
    }


    private void delDsZkConf(String[] nodes, String dsName) throws Exception {
        for (String confFilePath : nodes) {
            String zkPath = Constants.DBUS_ROOT + "/" + confFilePath;
            zkPath = zkPath.replace(KeeperConstants.DS_NAME_PLACEHOLDER, dsName);

            if (zkService.isExists(zkPath)) {
                zkConfService.deleteZkNodeOfPath(zkPath);
            }
            logger.info("delete zk nodes :" + zkPath + " ,success.");
        }
    }

    public ResultEntity getPath(String queryString) {
        ResponseEntity<ResultEntity> result = sender.get(KEEPER_SERVICE, "/datasource/topologies-jars", queryString);
        return result.getBody();
    }

    public Map viewLog(String topologyId) throws Exception {
        // 获取Topology运行所在worker及port
        if (!StormToplogyOpHelper.inited) {
            StormToplogyOpHelper.init(zkService);
        }

        Properties globalConf = zkService.getProperties(Constants.GLOBAL_PROPERTIES_ROOT);
        String stormHomePath = globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_HOME_PATH);
        //String host = globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOST);
        String port = globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_PORT);
        String user = globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_SSH_USER);

        String runningInfo = StormToplogyOpHelper.getTopoRunningInfoById(topologyId);
        if(StringUtils.isBlank(runningInfo)){
            Map resultMap = new HashMap<>();
            resultMap.put("runningInfo", runningInfo);
            resultMap.put("execResult", "");
            return resultMap;
        }
        String[] split = runningInfo.split(":");
        String command = "tail -300 " + stormHomePath + "/logs/workers-artifacts/" + split[1] + "/worker.log.creditease";

        String execResult = SSHUtils.executeCommand(user, split[0], Integer.parseInt(port), env.getProperty("pubKeyPath"), command, false);
        if (StringUtils.isBlank(execResult)) {
            command = "tail -300 " + stormHomePath + "/logs/workers-artifacts/" + split[1] + "/worker.log";
            execResult = SSHUtils.executeCommand(user, split[0], Integer.parseInt(port), env.getProperty("pubKeyPath"), command, false);
        }
        Map resultMap = new HashMap<>();
        resultMap.put("runningInfo", runningInfo);
        resultMap.put("execResult", execResult);
        return resultMap;
    }


    public int rerun(Integer dsId, String dsName, Long offset) throws Exception {
        String path = "/DBus/Topology/" + dsName + "-dispatcher/dispatcher.raw.topics.properties";
        byte[] data = zkService.getData(path);
        OrderedProperties orderedProperties = new OrderedProperties(new String(data));
        Object value = orderedProperties.get("dbus.dispatcher.offset");
        if (value != null && StringUtils.isNotBlank(value.toString()) && StringUtils.isNumeric(value.toString())) {
            return MessageCode.PLEASE_TRY_AGAIN_LATER;
        }
        orderedProperties.put("dbus.dispatcher.offset", offset);
        zkService.setData(path, orderedProperties.toString().getBytes());
        toolSetService.reloadConfig(dsId, dsName, "DISPATCHER_RELOAD_CONFIG");
        return 0;
    }

    /**
     * 自动部署ogg或者canal
     *
     * @param newOne
     */
    public int autoAddOggCanalLine(DataSource newOne) throws Exception {
        String dsType = newOne.getDsType();
        String dsName = newOne.getDsName();
        if (dsType.equalsIgnoreCase("mysql")) {
            if (autoDeployDataLineService.isAutoDeployCanal(dsName)) {
                return autoDeployDataLineService.addCanalLine(dsName, newOne.getCanalUser(), newOne.getCanalPass());
            }
        } else if (dsType.equalsIgnoreCase("oracle")) {
            if (autoDeployDataLineService.isAutoDeployOgg(dsName)) {
                return autoDeployDataLineService.addOracleLine(dsName);
            }
        }
        return 0;
    }

    /**
     * 自动部署ogg或者canal
     *
     * @param newOne
     */
    public int autoDeleteOggCanalLine(DataSource newOne) throws Exception {
        String dsType = newOne.getDsType();
        String dsName = newOne.getDsName();
        if (dsType.equalsIgnoreCase("mysql")) {
            if (autoDeployDataLineService.isAutoDeployCanal(dsName)) {
                return autoDeployDataLineService.delCanalLine(dsName);
            }
        } else if (dsType.equalsIgnoreCase("oracle")) {
            if (autoDeployDataLineService.isAutoDeployOgg(dsName)) {
                return autoDeployDataLineService.deleteOracleLine(dsName);
            }
        }
        return 0;
    }

    public DataSource getDataSourceByDsName(String dsName) {
        ResponseEntity<ResultEntity> result = sender.get(KEEPER_SERVICE, "/datasource/getByName", "?dsName=" + dsName);
        return result.getBody().getPayload(DataSource.class);
    }

}
