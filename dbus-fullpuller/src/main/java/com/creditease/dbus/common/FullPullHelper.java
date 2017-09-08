/*-
 * <<
 * DBus
 * ==
 * Copyright (C) 2016 - 2017 Bridata
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

package com.creditease.dbus.common;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

import com.creditease.dbus.commons.*;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.common.utils.DBConfiguration;
import com.creditease.dbus.common.utils.JsonUtil;
import com.creditease.dbus.commons.Constants.ZkTopoConfForFullPull;
import com.creditease.dbus.commons.exception.InitializationException;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.manager.GenericJdbcManager;
import com.creditease.dbus.manager.MySQLManager;
import com.creditease.dbus.manager.OracleManager;
import com.creditease.dbus.manager.SqlManager;
import com.esotericsoftware.minlog.Log;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FullPullHelper {
    private static Logger LOG = LoggerFactory.getLogger(FullPullHelper.class);
    private static ConcurrentMap<String, DBConfiguration> dbConfMap = new ConcurrentHashMap<>();

    private static volatile ZkService zkService = null;
    private static String topologyId = " ";
    private static String zkConnect;
    private static boolean isGlobal;
    private static String zkMonitorRootNodePath;

    public static final String RUNNING_CONF_KEY_COMMON = "common";
    public static final String RUNNING_CONF_KEY_CONSUMER = "consumer";
    public static final String RUNNING_CONF_KEY_BYTE_PRODUCER = "byte.producer";
    public static final String RUNNING_CONF_KEY_STRING_PRODUCER = "string.producer";
    public static final String RUNNING_CONF_KEY_STRING_PRODUCER_PROPS = "string.producer.props";
    public static final String RUNNING_CONF_KEY_MAX_FLOW_THRESHOLD = "maxFlowThreshold";
    public static final String RUNNING_CONF_KEY_ZK_SERVICE = "zkService";
    
    public static synchronized void initialize(String topoId, String zkConString) {
        if (topologyId != null) {
            topologyId = topoId;
            zkConnect = zkConString;
            isGlobal = topologyId.toLowerCase().indexOf(ZkTopoConfForFullPull.GLOBAL_FULLPULLER_TOPO_PREFIX) != -1 ? true : false;
            if (isGlobal) {
                zkMonitorRootNodePath = Constants.FULL_PULL_MONITOR_ROOT_GLOBAL;
            } else {
                zkMonitorRootNodePath = Constants.FULL_PULL_MONITOR_ROOT;
            }
        }
    }
//    public static void refreshCache() {
//        dbConfMap = new ConcurrentHashMap<>();
//        
//        if(!zkServiceMap.isEmpty()){
//            for (Map.Entry<String, ZkService> entry : zkServiceMap.entrySet()) { 
//                try {
//                    entry.getValue().close();
//                }
//                catch (IOException e) {
//                    LOG.warn("ZK connection close failed. Exception:"+e);
//                }
//            }
//        }
//        zkServiceMap = new ConcurrentHashMap<>();
//    }
    
    /**
     * 获取指定配置文件的Properties对象
     * @param dataSourceInfo 配置文件名称,不加扩展名
     * @return 返回配置文件内容
     */
    public static synchronized DBConfiguration getDbConfiguration(String dataSourceInfo) {
        DBConfiguration dbConf = null;
        if(dbConfMap.containsKey(dataSourceInfo)) {
            dbConf = dbConfMap.get(dataSourceInfo);
        } else {
            dbConf = DBHelper.generateDBConfiguration(dataSourceInfo);
            dbConfMap.put(dataSourceInfo, dbConf);
        }
        return dbConf;
    }
    
    public static ZkService getZkService() {
        if(zkService != null) {
            return zkService;
        } else {
            synchronized (FullPullHelper.class) {
                if(zkService != null) {
                    return zkService;
                } else {
                    try {
                        zkService = new ZkService(zkConnect);
                    } catch (Exception e) {
                        LOG.error("ZK connect failed. Exception:" + e);
                    }
                    return zkService;
                }
            }
        }
    }


    public static boolean updateMonitorFinishPartitionInfo(ZkService zkService, String dbNameSpace, ProgressInfo objProgInfo) {
        String zkPath = FullPullHelper.buildZkPath(zkMonitorRootNodePath, dbNameSpace);
        try {
            for (int i = 0; i < 10; i++) {
                int nextVersion = FullPullHelper.updateZkNodeInfoWithVersion(zkService, zkPath, objProgInfo);
                if (nextVersion != -1)
                    break;

                //稍作休息重试
                Thread.sleep(200);
                ProgressInfo zkProgInfo = FullPullHelper.getMonitorInfoFromZk(zkService, dbNameSpace);
                objProgInfo.mergeProgressInfo(zkProgInfo);
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error("updateMonitorFinishPartitionInfo Failed.", e);
            return false;
        }
        return true;
    }

    public static void updateMonitorSplitPartitionInfo(ZkService zkService, String dbNameSpace, int totalShardsCount, int totalRows) {
        try {
            String currentTimeStampString = FullPullHelper.getCurrentTimeStampString();

            ProgressInfo progressObj = FullPullHelper.getMonitorInfoFromZk(zkService, dbNameSpace);
            progressObj.setUpdateTime(currentTimeStampString);
            progressObj.setTotalCount(String.valueOf(totalShardsCount));
            progressObj.setTotalRows(String.valueOf(totalRows));

            long curSecs = System.currentTimeMillis() / 1000;
            long startSecs = progressObj.getStartSecs() == null ? curSecs : Long.parseLong(progressObj.getStartSecs());
            long consumeSecs = curSecs - startSecs;
            progressObj.setConsumeSecs(String.valueOf(consumeSecs) + "s");

            ObjectMapper mapper = JsonUtil.getObjectMapper();
            String progressNodePath = FullPullHelper.buildZkPath(zkMonitorRootNodePath, dbNameSpace);
            FullPullHelper.updateZkNodeInfo(zkService, progressNodePath, mapper.writeValueAsString(progressObj));
        } catch (Exception e) {
            LOG.error("Update Monitor Detail Info Failed.", e);
            throw new RuntimeException(e);
        }
    }

    public static ProgressInfo getMonitorInfoFromZk(ZkService zkService, String dbNameSpace) throws Exception {
        String zkPath = buildZkPath(zkMonitorRootNodePath, dbNameSpace);
        int version = zkService.getVersion(zkPath);
        if(version != -1) {
            byte[] data = zkService.getData(zkPath);

            if (data != null) {
                ObjectMapper mapper = JsonUtil.getObjectMapper();
                ProgressInfo progressObj = JsonUtil.convertToObject(mapper, new String(data), ProgressInfo.class);
                progressObj.setZkVersion(version);
                return progressObj;
            }
        }
        return (new ProgressInfo());
    }

    public static void startPullReport(ZkService zkService, String dataSourceInfo) {
        try {
            String currentTimeStampString = FullPullHelper.getCurrentTimeStampString();
            long startSecs = System.currentTimeMillis() / 1000;

            JSONObject wrapperObj = JSONObject.parseObject(dataSourceInfo);
            JSONObject payloadObj = wrapperObj.getJSONObject("payload");
            String version = payloadObj.getString(DataPullConstants.FullPullInterfaceJson.VERSION_KEY);
            String batchNo = payloadObj.getString(DataPullConstants.FullPullInterfaceJson.BATCH_NO_KEY);


            DBConfiguration dbConf = FullPullHelper.getDbConfiguration(dataSourceInfo);
            ObjectMapper mapper = JsonUtil.getObjectMapper();
            FullPullHelper.createProgressZkNode(zkService, dbConf);

            if (!isGlobal) {
                HeartBeatInfo heartBeanObj = new HeartBeatInfo();
                heartBeanObj.setArgs(dbConf.getDbNameAndSchema());
                heartBeanObj.setCmdType(String.valueOf(CommandType.FULL_PULLER_BEGIN.getCommand()));
                FullPullHelper.updateZkNodeInfo(zkService, Constants.HEARTBEAT_CONTROL_NODE, mapper.writeValueAsString(heartBeanObj));
            }

            ProgressInfo progressObj = new ProgressInfo();
            progressObj.setCreateTime(currentTimeStampString);
            progressObj.setStartTime(currentTimeStampString);
            progressObj.setUpdateTime(currentTimeStampString);
            progressObj.setStartSecs(String.valueOf(startSecs));
            progressObj.setStatus(Constants.FULL_PULL_STATUS_SPLITTING);
            progressObj.setVersion(version);
            progressObj.setBatchNo(batchNo);
            String progressInfoNodePath = FullPullHelper.getMonitorNodePath(dataSourceInfo);
            FullPullHelper.updateZkNodeInfo(zkService, progressInfoNodePath, mapper.writeValueAsString(progressObj));

            FullPullHelper.updateFullPullReqTable(dataSourceInfo, FullPullHelper.getCurrentTimeStampString(), null, null, null);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            String errorMsg = "Encountered exception when writing msg to zk monitor.";
            LOG.error(errorMsg, e);
            throw new RuntimeException(e);
        }
    }

    //完成拉取
    public static void finishPullReport(ZkService zkService, String dataSourceInfo, String completedTime, String pullStatus, String errorMsg) {
        DBConfiguration dbConf = getDbConfiguration(dataSourceInfo);
        // 通知相关方全量拉取完成：包括以下几个部分。
        try {
            // 一、向zk监控信息写入完成
            if (!isGlobal) {
                HeartBeatInfo heartBeanObj = new HeartBeatInfo();
                heartBeanObj.setArgs(dbConf.getDbNameAndSchema());
                heartBeanObj.setCmdType(String.valueOf(CommandType.FULL_PULLER_END.getCommand()));

                ObjectMapper mapper = JsonUtil.getObjectMapper();
                updateZkNodeInfo(zkService, Constants.HEARTBEAT_CONTROL_NODE, mapper.writeValueAsString(heartBeanObj));
            }

            // 二、通知monitorRoot节点和monitor节点
            if(!pullStatus.equals(Constants.DataTableStatus.DATA_STATUS_OK)) {
                sendErrorMsgToZkMonitor(zkService, dataSourceInfo, errorMsg);
            }

            if (!isGlobal) {
                // 三、将完成时间回写到数据库，通知业务方
                SqlManager dbManager = null;
                try {
                    dbManager = FullPullHelper.getDbManager(dbConf, dbConf.getString(DBConfiguration.DataSourceInfo.URL_PROPERTY_READ_WRITE));
                    dbManager.writeBackOriginalDb(null, completedTime, pullStatus, errorMsg);
                }
                catch (Exception e) {
                    Log.error("Error occurred when report full data pulling status to original DB.");
                    pullStatus = Constants.DataTableStatus.DATA_STATUS_ABORT;
                }finally {
                    try {
                        dbManager.close();
                    } catch (Exception e) {
                        Log.error("finally", e);
                    }
                }
            }

            if (!isGlobal) {
                // 四、向约定topic发送全量拉取完成或出错终止信息，供增量拉取进程使用
                sendAckInfoToCtrlTopic(dataSourceInfo, completedTime, pullStatus);
            }

        }
        catch (Exception e) {
            LOG.error("finishPullReport() Exception:", e);
            throw new RuntimeException(e);
        }

    }

    private static void sendErrorMsgToZkMonitor(ZkService zkService, String dataSourceInfo, String errorMsg) throws Exception {
        String currentTimeStampString = FullPullHelper.getCurrentTimeStampString();

        DBConfiguration dbConf = getDbConfiguration(dataSourceInfo);
        String dbNameSpace = dbConf.buildSlashedNameSpace(dataSourceInfo);

        ProgressInfo progressObj = FullPullHelper.getMonitorInfoFromZk(zkService, dbNameSpace);
        progressObj.setUpdateTime(currentTimeStampString);
        String oriErrorMsg = progressObj.getErrorMsg();
        errorMsg = StringUtils.isBlank(oriErrorMsg) ? errorMsg : oriErrorMsg + ";" + errorMsg;
        progressObj.setErrorMsg(errorMsg);

        ObjectMapper mapper = JsonUtil.getObjectMapper();
        String progressNodePath = getMonitorNodePath(dataSourceInfo);
        updateZkNodeInfo(zkService, progressNodePath, mapper.writeValueAsString(progressObj));
    }

    public static void updateZkNodeInfo(ZkService zkService, String zkPath, String updateInfo) throws Exception {
        if(zkService == null || StringUtils.isBlank(zkPath) || StringUtils.isBlank(updateInfo)) {
            return;
        }
        if(!zkService.isExists(zkPath)) {
            zkService.createNode(zkPath, null);
        }
        zkService.setData(zkPath, updateInfo.getBytes());
    }

    public static int updateZkNodeInfoWithVersion(ZkService zkService, String zkPath, ProgressInfo objProgInfo) throws Exception{
        if(zkService == null || StringUtils.isBlank(zkPath)) {
            return -1;
        }

        if(!zkService.isExists(zkPath)) {
            zkService.createNode(zkPath, null);
        }

        ObjectMapper mapper = JsonUtil.getObjectMapper();
        String updateInfo = mapper.writeValueAsString(objProgInfo);
        int nextVersion = zkService.setDataWithVersion(zkPath, updateInfo.getBytes(), objProgInfo.getZkVersion());
        if (nextVersion != -1) {
            objProgInfo.setZkVersion(nextVersion);
        }
        return nextVersion;
    }


    public static void createProgressZkNode(ZkService zkService, DBConfiguration dbConf) {
        String dbName = (String)(dbConf.getConfProperties().get(DBConfiguration.DataSourceInfo.DB_NAME));
        String dbSchema = (String)(dbConf.getConfProperties().get(DBConfiguration.DataSourceInfo.DB_SCHEMA));
        String tableName = (String)(dbConf.getConfProperties().get(DBConfiguration.DataSourceInfo.TABLE_NAME));
        String zkPath;
        try {
            zkPath = zkMonitorRootNodePath;
            if(!zkService.isExists(zkPath)){
                zkService.createNode(zkPath, null);
            }
            zkPath = buildZkPath(zkMonitorRootNodePath, dbName);
            if(!zkService.isExists(zkPath)){
                zkService.createNode(zkPath, null);
            }
            zkPath = buildZkPath(zkMonitorRootNodePath, dbName + "/" + dbSchema);
            if(!zkService.isExists(zkPath)){
                zkService.createNode(zkPath, null);
            }
            zkPath = buildZkPath(zkMonitorRootNodePath, dbName+ "/" + dbSchema + "/" + tableName);
            if(!zkService.isExists(zkPath)){
                zkService.createNode(zkPath, null);
            }
        } catch (Exception e) {
            Log.error("create parent node failed exception!", e);
            throw new RuntimeException(e);
        }
    }

    //TODO 应该放到 util 类中
    public static String getCurrentTimeStampString() {
        SimpleDateFormat formatter = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss.SSS");
        String timeStampString = formatter.format(new Date()); 
        return timeStampString;
    }
    
    private static String buildZkPath(String parent, String child) {
        return parent + "/" + child;
    }

    public static String getMonitorNodePath(String dataSourceInfo) {
        // 根据当前拉取全量的目标数据库信息，生成数据库表NameSpace信息
        DBConfiguration dbConf = FullPullHelper.getDbConfiguration(dataSourceInfo);
        String dbNameSpace = dbConf.buildSlashedNameSpace(dataSourceInfo);

        // 获得全量拉取监控信息的zk node path
        String monitorNodePath = FullPullHelper.buildZkPath(zkMonitorRootNodePath, dbNameSpace);
        return monitorNodePath;
    }

    public static String getDbNameSpace(String dataSourceInfo) {
        DBConfiguration dbConf = FullPullHelper.getDbConfiguration(dataSourceInfo);
        String dbNameSpace = dbConf.buildSlashedNameSpace(dataSourceInfo);
        return dbNameSpace;
    }

    public static boolean validateMetaCompatible(String dataSourceInfo) {
        try {
            DBConfiguration dbConf = getDbConfiguration(dataSourceInfo);
            // Mysql 不校验meta
            if(DbusDatasourceType.MYSQL.name().equalsIgnoreCase(dbConf.getString(DBConfiguration.DataSourceInfo.DS_TYPE))) {
                return true;
            }
            LOG.info("Not mysql. Should validate meta.");

            //从源库中获取meta信息
            OracleManager oracleManager = (OracleManager)FullPullHelper.getDbManager(dbConf, dbConf.getString(DBConfiguration.DataSourceInfo.URL_PROPERTY_READ_ONLY));
            MetaWrapper metaInOriginalDb = oracleManager.queryMetaInOriginalDb();

            //从dbus manager库中获取meta信息
            MetaWrapper metaInDbus = DBHelper.getMetaInDbus(dataSourceInfo);

            //从dbus manager库中获取不到meta信息，直接返回false
            if(metaInDbus == null) {
                LOG.info("[Mysql manager] metaInDbus IS NULL.");
                return false;
            }

            //验证源库和管理库中的meta信息是否兼容
            return MetaWrapper.isCompatible(metaInOriginalDb, metaInDbus);
        }
        catch (Exception e) {
            LOG.error("Failed on camparing meta.",e);
            return false;
        }
    }


    private static void sendAckInfoToCtrlTopic(String dataSourceInfo, String completedTime, String pullStatus) {
        try {
            // 在源dataSourceInfo的基础上，更新全量拉取相关信息。然后发回src topic
            JSONObject jsonObj = JSONObject.parseObject(dataSourceInfo);
            jsonObj.put(DataPullConstants.FullPullInterfaceJson.FROM_KEY, DataPullConstants.FullPullInterfaceJson.FROM_VALUE);
            jsonObj.put(DataPullConstants.FullPullInterfaceJson.TYPE_KEY, DataPullConstants.FullPullInterfaceJson.TYPE_VALUE);
            // notifyFullPullRequestor
            JSONObject payloadObj = jsonObj.getJSONObject(DataPullConstants.FullPullInterfaceJson.PAYLOAD_KEY);
            // 完成时间
            payloadObj.put(DataPullConstants.FullPullInterfaceJson.COMPLETE_TIME_KEY, completedTime);
            // 拉取是否成功标志位
            payloadObj.put(DataPullConstants.FullPullInterfaceJson.DATA_STATUS_KEY, pullStatus);
            jsonObj.put(DataPullConstants.FullPullInterfaceJson.PAYLOAD_KEY, payloadObj);
            String ctrlTopic = getFullPullProperties(Constants.ZkTopoConfForFullPull.COMMON_CONFIG, true)
                .getProperty(Constants.ZkTopoConfForFullPull.FULL_PULL_SRC_TOPIC);
            Producer producer = DbusHelper
                    .getProducer(getFullPullProperties(Constants.ZkTopoConfForFullPull.BYTE_PRODUCER_CONFIG, true));
            ProducerRecord record = new ProducerRecord<>(ctrlTopic, DataPullConstants.FullPullInterfaceJson.TYPE_VALUE, jsonObj.toString().getBytes());
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata meta = future.get();
        }
        catch (Exception e) {
            Log.error("Error occurred when report full data pulling status.", e);
            throw new RuntimeException(e);
        }
    }
    
    public static GenericJdbcManager getDbManager(DBConfiguration dbConf, String url) {
        String datasourceType = dbConf.getString(DBConfiguration.DataSourceInfo.DS_TYPE);
        String dbDriverClass = DbusDatasourceType.getDataBaseDriverClass(DbusDatasourceType.valueOf(datasourceType.toUpperCase())); 
        DbusDatasourceType dataBaseType = DbusDatasourceType.valueOf(datasourceType.toUpperCase());
        switch (dataBaseType) {
            case ORACLE:
                return  new OracleManager(dbConf, url);
            case MYSQL:
                return new MySQLManager(dbConf, url);
            default:
                return new GenericJdbcManager(dbDriverClass, dbConf, url);
        }
        
    }
    
//    private static String buildOracleManagerCacheKey(String dataSourceInfo, String url){
//        return dataSourceInfo+url;
//    }
    
 /*   public static  synchronized void closeOracleManager(String dataSourceInfo, String url) {
        String key = buildOracleManagerCacheKey(dataSourceInfo, url);
        OracleManager oracleManager = oracleManagerMap.get(key);
        if(oracleManager!=null){
            try {
                oracleManager.close();
                Iterator iterator = oracleManagerMap.keySet().iterator();   
                while (iterator.hasNext()) {  
                    String oracleManagerKey = (String) iterator.next();  
                    if (key.equals(oracleManagerKey)) {  
                       iterator.remove();// 必须有，否则抛 java.util.ConcurrentModificationException 异常哦
                       oracleManagerMap.remove(key);      
                     }  
                 }  
            }
            catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }*/



    /**
     * 更新数据库全量拉取请求表
     */
    public static void updateFullPullReqTable(String dataSourceInfo, String startTime, String completedTime, String pullStatus, String errorMsg){
        SqlManager dbManager = null;
        try {
            DBConfiguration dbConf = FullPullHelper.getDbConfiguration(dataSourceInfo); 
            String readWriteDbUrl = dbConf.getString(DBConfiguration.DataSourceInfo.URL_PROPERTY_READ_WRITE);
            dbManager = getDbManager(dbConf, readWriteDbUrl);
            dbManager.writeBackOriginalDb(startTime, completedTime, pullStatus, errorMsg);
        }
        catch (Exception e) {
            LOG.error("Error occured when report full data pulling status to original DB.");
        }finally {
            try {
                dbManager.close();
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
    
    public static Properties getFullPullProperties(String confFileName, boolean useCacheIfCached) {
        String customizeConfPath = topologyId + "/" + confFileName;
        LOG.info("customizeConfPath is {}", customizeConfPath);
        try {
            Properties properties = PropertiesHolder.getPropertiesInculdeCustomizeConf(confFileName, customizeConfPath, useCacheIfCached);
            if(Constants.ZkTopoConfForFullPull.CONSUMER_CONFIG.equals(confFileName)) {
                properties.put("client.id", topologyId + "_" + properties.getProperty("client.id") + System.currentTimeMillis());
                properties.put("group.id", topologyId + "_" + properties.getProperty("group.id") + System.currentTimeMillis());
            }
            return properties;
        }
        catch (Exception e) {
            return new Properties();
        }
    }
    
    public static String getConfFromZk(String confFileName, String propKey) {
        Properties properties = getFullPullProperties(confFileName, false);
        return properties.getProperty(propKey); 
    }     
    
    public static String getDataSourceKey(JSONObject ds) {
        StringBuilder key= new StringBuilder();
        JSONObject jsonObj = ds.getJSONObject(DataPullConstants.FullPullInterfaceJson.PAYLOAD_KEY);
        key.append(jsonObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_SCHEMA_NAME)).append("_");
        key.append(jsonObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_TABLE_NAME)).append("_");
        key.append("s").append(jsonObj.getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_SEQNO)).append("_");
        key.append("v").append(jsonObj.getString(DataPullConstants.FullPullInterfaceJson.VERSION_KEY));
        return key.toString();
    }

    private static String makePendingTaskKey(String dataSourceInfo) {
        assert(dataSourceInfo != null);

        // 生成用于记录pending task的key。
        JSONObject dsObj = JSONObject.parseObject(dataSourceInfo)
                .getJSONObject(DataPullConstants.FullPullInterfaceJson.PAYLOAD_KEY);
        /*
         * 整个程序流转dataSourceInfo的全息信息很重要，所以利用dataSourceInfo来生成key。
         * 但dataSourceInfo在处理过程中发生了变化，多了VERSION field（目前变化的部分只有VERSION）。
         * 为了保证同一个pending task的监控key一致，以便ADD和REMOVE操作能对应上，需要剔除变化的部分。
         * 这样做看起来不是特别好，但没有找到更好的方法。
         */
        dsObj.remove("VERSION");

        // 双引号在存储到zk时，会带转义符。为了避免其带来的负面影响，key剔除所有双引号，以便于比较。
        // 这个方法中没有用 JSONArray，然后直接利用其
        // contains，add，remove等方法处理pending任务的记录和移出，原因也在于转义符导致key对不上。
        // 原始的dataSourceInfo还需用来进行resume
        // message的发送等，需保留其原样，所以放弃JSONArray这种数据结构,后续采用JSONObject put
        // key-value对的方式来处理。
        String pendingTaskKey = dsObj.toString().replace("\"", "");
        return pendingTaskKey;
    }
    
    /**
     * 第三个参数dataSourceInfo可能传null值，此时操作类型一定是FULLPULL_PENDING_TASKS_OP_CRASHED_NOTIFY。
     * crash检查是全局的，而不是针对某一个datasource。所以无特定的dataSourceInfo。
     * crash的处理未用到dataSourceInfo参数，所以这种情况下传null不影响逻辑。
     * */   
    public static void updatePendingTasksTrackInfo(ZkService zkService, String dsName,
                                                   String dataSourceInfo, String opType) {
        try {
            zkService = getZkService();
            //1 get pendingNodPath
            String pendingZKNodePath;
            if (isGlobal) {
                pendingZKNodePath = zkMonitorRootNodePath;
            } else {
                pendingZKNodePath = zkMonitorRootNodePath + "/" + dsName;
            }
            if (!zkService.isExists(pendingZKNodePath)) {
                LOG.error("Not found zk node of path '{}' when checking pending tasks.", pendingZKNodePath);
                return;
            }

            JSONObject pendingNodeContent = null;
            byte[] pendingNodeContentByte = zkService.getData(pendingZKNodePath);
            if(pendingNodeContentByte != null && pendingNodeContentByte.length > 0) {
                pendingNodeContent = JSONObject.parseObject(new String(pendingNodeContentByte));
            } else {
                pendingNodeContent = new JSONObject();
            }


            // 2 get pendingTask
            // 引入topologyId做key，以免pullling和splitting的消息相互交叉覆盖，作区分
            JSONObject pendingTasksJsonObj;
            String pendingTasksJsonObjKey = DataPullConstants.FULLPULL_PENDING_TASKS + topologyId;
            if(pendingNodeContent.containsKey(pendingTasksJsonObjKey)) {
                pendingTasksJsonObj = pendingNodeContent.getJSONObject(pendingTasksJsonObjKey);
            } else {
                pendingTasksJsonObj = new JSONObject();
            }


            if (DataPullConstants.FULLPULL_PENDING_TASKS_OP_ADD_WATCHING.equals(opType)) {
                String pendingTaskKey = makePendingTaskKey(dataSourceInfo);
                if (!pendingTasksJsonObj.containsKey(pendingTaskKey)) {
                    pendingTasksJsonObj.put(pendingTaskKey, dataSourceInfo);
                    pendingNodeContent.put(pendingTasksJsonObjKey, pendingTasksJsonObj);
                    LOG.info("Fullpull task [{}] come from {} is added to watching list!", dataSourceInfo, topologyId);
                } else {
                    //not suppose to here
                    LOG.warn("duplicate task [{}] come from {} i existed in watching list!", dataSourceInfo, topologyId);
                }

            } else if (DataPullConstants.FULLPULL_PENDING_TASKS_OP_REMOVE_WATCHING.equals(opType)) {
                String pendingTaskKey = makePendingTaskKey(dataSourceInfo);
                pendingTasksJsonObj.remove(pendingTaskKey);
                pendingNodeContent.put(pendingTasksJsonObjKey, pendingTasksJsonObj);
                
                LOG.info("Fullpull task [{}] is removing from watching list by {}!", dataSourceInfo, topologyId);

            } else if (DataPullConstants.FULLPULL_PENDING_TASKS_OP_CRASHED_NOTIFY.equals(opType)) {
                // 检查是否有遗留未决的拉取任务。如果有，需要resolve（发resume消息通知给appender，并在zk上记录，且将监测到的pending任务写日志，方便排查问题）。
                if (pendingTasksJsonObj != null && !pendingTasksJsonObj.isEmpty()) {
                    for (Map.Entry<String, Object> entry : pendingTasksJsonObj.entrySet()) {
                        String pendingDataSourceInfo = (String) entry.getValue();

                        //构建错误msg等
                        JSONObject dataSourceInfoJsonObj = JSONObject.parseObject(pendingDataSourceInfo);
                        String schema = dataSourceInfoJsonObj
                                .getJSONObject(DataPullConstants.FullPullInterfaceJson.PAYLOAD_KEY)
                                .getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_SCHEMA_NAME);
                        String table = dataSourceInfoJsonObj
                                .getJSONObject(DataPullConstants.FullPullInterfaceJson.PAYLOAD_KEY)
                                .getString(DataPullConstants.FULL_DATA_PULL_REQ_PAYLOAD_TABLE_NAME);
                        String schemaAndTableInfo = String.format("%s.%s", schema, table);

                        String errorMsg = String.format("Pending fullpull task for [%s] is found by %s.",
                                schemaAndTableInfo, topologyId);

                        //发送abort
                        finishPullReport(zkService, pendingDataSourceInfo,
                                FullPullHelper.getCurrentTimeStampString(), Constants.DataTableStatus.DATA_STATUS_ABORT,
                                errorMsg);

                        errorMsg = String.format("Pending fullpull task for [%s] found by %s is resolved.",
                                schemaAndTableInfo, topologyId);
                        LOG.info(errorMsg);
                    }

                    // 对于已经resolve的pending任务，将其移出pending队列，以免造成无限重复处理。
                    pendingNodeContent.put(pendingTasksJsonObjKey, new JSONObject());// 清空pending队列
                } else {
                    LOG.info("OK! no pending job.");
                }
            }
            zkService = getZkService();
            try{
                zkService.setData(pendingZKNodePath, pendingNodeContent.toString().getBytes());
            }catch (Exception e) {
                e.printStackTrace();
            }

            LOG.info("Pending Tasks Watching Node on zk is updated!");
        } catch (Exception e) {
            LOG.error("Maintaining pending tasks encounter error!", e);
        }
    }

    public static Map loadConfProps(String zkconnect, String topid, String zkTopoRoot, String consumerSubTopicKey) {
        Map confMap = new HashMap<>();
        try {
            //连接zk
            PropertiesHolder.initialize(zkconnect, zkTopoRoot);
            initialize(topid, zkconnect);
            //从zk中获取common-config节点的内容
            Properties commonProps = getFullPullProperties(Constants.ZkTopoConfForFullPull.COMMON_CONFIG, true);
            //创建zk连接，将其存入confMap中
            ZkService zkService = getZkService();
            //最大流门限值
            Integer maxFlowThreshold = Integer.valueOf(commonProps.getProperty(Constants.ZkTopoConfForFullPull.SPOUT_MAX_FLOW_THRESHOLD));
            LOG.info("maxFlowThreshold is {} on FullPullHelper.loadConfProps", maxFlowThreshold);
            //加入confMap
            confMap.put(RUNNING_CONF_KEY_COMMON, commonProps);
            confMap.put(RUNNING_CONF_KEY_MAX_FLOW_THRESHOLD, maxFlowThreshold);
            confMap.put(RUNNING_CONF_KEY_ZK_SERVICE, zkService);
            
            if(StringUtils.isNotBlank(consumerSubTopicKey)) {
                String consumerSubTopic = commonProps.getProperty(consumerSubTopicKey);
                Properties consumerProps = getFullPullProperties(Constants.ZkTopoConfForFullPull.CONSUMER_CONFIG, true);
                Consumer<String, byte[]> consumer = DbusHelper.createConsumer(consumerProps, consumerSubTopic);
                confMap.put(RUNNING_CONF_KEY_CONSUMER, consumer);
            }
            
            Properties byteProducerProps = FullPullHelper.getFullPullProperties(Constants.ZkTopoConfForFullPull.BYTE_PRODUCER_CONFIG, true);
            if(null != byteProducerProps) {
                Producer byteProducer = DbusHelper.getProducer(byteProducerProps);
                confMap.put(RUNNING_CONF_KEY_BYTE_PRODUCER, byteProducer);
            }
            
            Properties stringProducerProps = FullPullHelper.getFullPullProperties(Constants.ZkTopoConfForFullPull.STRING_PRODUCER_CONFIG, true);
            if(null != stringProducerProps) {
                Producer stringProducer = DbusHelper.getProducer(stringProducerProps);
                confMap.put(RUNNING_CONF_KEY_STRING_PRODUCER, stringProducer);
                confMap.put(RUNNING_CONF_KEY_STRING_PRODUCER_PROPS, stringProducerProps);
            }
        } catch (Exception e) {
            LOG.error("Encountered exception:", e);
            throw new InitializationException();
        }
        return confMap;
    }

    @SuppressWarnings("unchecked")
    public static Map reloadZkServiceConfProps(String zkconnect, String zkTopoRoot) {
        Map confMap = new HashMap<>();
        try{
            PropertiesHolder.initialize(zkconnect, zkTopoRoot);
            Properties commonProps = getFullPullProperties(Constants.ZkTopoConfForFullPull.COMMON_CONFIG, true);
            String zkConString = commonProps.getProperty(Constants.ZkTopoConfForFullPull.FULL_PULL_MONITORING_ZK_CONN_STR);

            ZkService zkService = null;
            try {
                zkService = new ZkService(zkConString);
            } catch (Exception e) {
                LOG.error("Create new zkservice failed. Exception:" + e);
            }
            confMap.put(RUNNING_CONF_KEY_ZK_SERVICE, zkService);
        } catch (Exception e) {
            LOG.error("Encountered exception:", e);
            throw new InitializationException();
        }
        return confMap;
    }


    public static void saveReloadStatus(String json, String title, boolean prepare, String zkServers) {
        if (json != null) {
            String msg = title + " reload successful!";
            ControlMessage message = ControlMessage.parse(json);
            CtlMessageResult result = new CtlMessageResult(title, msg);
            result.setOriginalMessage(message);
            CtlMessageResultSender sender = new CtlMessageResultSender(message.getType(), zkServers);
            sender.send(title, result, prepare, false);
            LOG.info("Write reload status to zk successfully. Msg:{}", msg);
        }
    }
}
