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
import com.creditease.dbus.constant.ServiceNames;
import com.creditease.dbus.domain.model.DataSource;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.utils.DelZookeeperNodesTemplate;
import com.creditease.dbus.utils.StormToplogyOpHelper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
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

    private static final String KEEPER_SERVICE = ServiceNames.KEEPER_SERVICE;

    private final String OBEJCT_COLUMN = "--------";//查询的dbusData不是table，是存储过程时对应的column位置的值

    /**
     * datasource首页的搜索
     * @param queryString param:dsName,if ds=null get all
     */
    public ResultEntity search(String queryString) throws Exception{
        ResponseEntity<ResultEntity> result;
        if(queryString==null || queryString.isEmpty()){
            result= sender.get(KEEPER_SERVICE, "/datasource/search");
        }else {
            queryString = URLDecoder.decode(queryString,"UTF-8");
            result = sender.get(KEEPER_SERVICE, "/datasource/search", queryString);
        }
        return result.getBody();
    }

    public ResultEntity getById(Integer id){
        ResponseEntity<ResultEntity> result = sender.get(KEEPER_SERVICE,"/datasource/{id}",id);
        return result.getBody();
    }

    public ResultEntity insertOne(DataSource dataSource){
        ResponseEntity<ResultEntity> result = sender.post(KEEPER_SERVICE,"/datasource",dataSource);
        return result.getBody();
    }

    public ResultEntity update(DataSource dataSource){
        ResponseEntity<ResultEntity> result = sender.post(KEEPER_SERVICE,"/datasource/update",dataSource);
        return result.getBody();
    }

    public int countByDsId(Integer id){
		//是否还有项目在使用
		return sender.get(KEEPER_SERVICE, "/projectTable/count-by-ds-id/{id}", id)
				.getBody().getPayload(Integer.class);
	}

    public ResultEntity delete(Integer id){
        DataSource dataSource = this.getById(id).getPayload(DataSource.class);
        String dsName = dataSource.getDsName();
		//删除zk节点
		try {
            delDsZkConf(DelZookeeperNodesTemplate.ZK_CLEAR_NODES_PATHS, dsName);
            delDsZkConf(DelZookeeperNodesTemplate.ZK_CLEAR_NODES_PATHS_OF_DSNAME_TO_UPPERCASE, dsName.toUpperCase());
        } catch (Exception e) {
			logger.error(e.getMessage(),e);
        }
        //级联删除相关表数据
		return sender.get(KEEPER_SERVICE, "/datasource/delete/{id}", id).getBody();
    }

    public ResultEntity getDataSourceByName(String name){
        ResponseEntity<ResultEntity> result = sender.get(KEEPER_SERVICE,"/datasource/getDataSourceByName",name);
        return result.getBody();
    }

    public ResultEntity searchFromSource(Integer dsId){
        ResultEntity result =getById(dsId);
        if(!result.success()){
            return result;
        }
        //根据dsId获取需要的参数信息
        DataSource dataSource =result.getPayload(new TypeReference<DataSource>() {});
        Map<String,Object> params = new HashedMap(5);
        params.put("dsId",dsId);
        params.put("dsType",dataSource.getDsType());
        params.put("URL",dataSource.getMasterUrl());
        params.put("user",dataSource.getDbusUser());
        params.put("password",dataSource.getDbusPwd());
        //调用接口查询
        ResponseEntity<ResultEntity> responseEntity = sender.post(KEEPER_SERVICE,"/datasource/searchFromSource",params);
        if(!responseEntity.getStatusCode().is2xxSuccessful() || !responseEntity.getBody().success()){
            return responseEntity.getBody();
        }
        List<String> resultList = responseEntity.getBody().getPayload(new TypeReference<List<String>>() {});
        //将结果数据格式化成前端需要的数据
        List<String> structureList = new ArrayList<>();//格式化完的结果

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
        }

        //result.setPayload(structureList);
        result = new ResultEntity();
        result.setPayload(structureList);
        return result;
    }

    public ResultEntity validateDataSources(Map<String,Object> map){
        ResponseEntity<ResultEntity> result = sender.post(KEEPER_SERVICE,"/datasource/validate",map);
        return result.getBody();
    }

    public ResultEntity modifyDataSourceStatus(Long id, String status) {
        ResponseEntity<ResultEntity> result = sender.get(KEEPER_SERVICE,"/datasource/{id}/{status}",id, status);
        return result.getBody();
    }

    public ResultEntity getDSNames() {
        ResponseEntity<ResultEntity> result = sender.get(KEEPER_SERVICE,"/datasource/getDSNames");
        return result.getBody();
    }

    public String startTopology(String dsName, String jarPath, String jarName, String topologyType) throws Exception {
        Properties globalConf = zkService.getProperties(KeeperConstants.GLOBAL_CONF);

        String hostIp =  globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOST);
        String port =  globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_PORT);
        String stormBaseDir =  globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_HOME_PATH);
		String stormSshUser = globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_SSH_USER);

        String cmd = "cd "+ stormBaseDir + "/" + KeeperConstants.STORM_JAR_DIR +";";
        cmd +=  " ./dbus_startTopology.sh " + stormBaseDir + " " + topologyType + " " + env.getProperty("zk.str");
        cmd +=  " " + dsName + " " + jarPath + " " + jarName;
        // String sshCmd = "ssh -p " + port + " " + stormSshUser + "@" + hostIp + " " + "'" + cmd + "'";

        logger.info("Topology Start Command:{}", cmd);
        return StormToplogyOpHelper.execute(env.getProperty("pubKeyPath"), stormSshUser, hostIp, Integer.parseInt(port), cmd);
    }

    public static void main(String[] args) {
        // System.out.println(startTopology("testByccSSH","./0.4.x/splitter_puller/20180201_094712/","dbus-fullpuller_1.3-0.4.0-jar-with-dependencies.jar","splitter-puller"));
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

    public ResultEntity getPath(String queryString){
        ResponseEntity<ResultEntity> result = sender.get(KEEPER_SERVICE,"/datasource/topologies-jars",queryString);
        return result.getBody();
    }

    public Map viewLog(String topologyId) throws Exception{
        // 获取Topology运行所在worker及port
        if (!StormToplogyOpHelper.inited) {
            StormToplogyOpHelper.init(zkService);
        }
        String runningInfo = StormToplogyOpHelper.getTopoRunningInfoById(topologyId);
        Properties globalConf = zkService.getProperties(Constants.GLOBAL_PROPERTIES_ROOT);
        String stormHomePath = globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_HOME_PATH);
        String host =  globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_HOST);
        String port =  globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_NIMBUS_PORT);
        String user = globalConf.getProperty(KeeperConstants.GLOBAL_CONF_KEY_STORM_SSH_USER);

        String command = "tail -200 " + stormHomePath + "logs/workers-artifacts/" +
                runningInfo.substring(runningInfo.indexOf(":", 1) + 1, runningInfo.length()) + "/worker.log.creditease";

        String execResult = executeCommand(user, host, Integer.parseInt(port), env.getProperty("pubKeyPath"), command);
        Map resultMap = new HashMap<>();
        resultMap.put("runningInfo",runningInfo);
        resultMap.put("execResult",execResult);
        return resultMap;
    }

    private String executeCommand(String username, String host, int port, String pubKeyPath, String command) {
        String  result = "";
        Session session = null;
        ChannelExec channel = null;
        try {
            JSch jsch = new JSch();
            jsch.addIdentity(pubKeyPath);

            session = jsch.getSession(username, host, port);
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();
            channel = (ChannelExec) session.openChannel("exec");
            channel.setCommand(command);

            BufferedReader in = new BufferedReader(new InputStreamReader(channel.getInputStream()));
            channel.connect();
            String msg;
            StringBuilder sb = new StringBuilder();
            while ((msg = in.readLine()) != null) {
                sb.append(msg).append("\n");
            }
            return sb.toString();
        } catch (Exception e) {
            logger.error(e.getMessage(),e);
            return result;
        } finally {
            if (channel != null) {
                channel.disconnect();
            }
            if (session != null) {
                session.disconnect();
            }
        }
    }
}
