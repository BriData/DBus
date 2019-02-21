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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.domain.mapper.ProjectEncodeHintMapper;
import com.creditease.dbus.domain.mapper.ProjectMapper;
import com.creditease.dbus.domain.mapper.ProjectResourceMapper;
import com.creditease.dbus.domain.mapper.ProjectTopoTableEncodeOutputColumnsMapper;
import com.creditease.dbus.domain.mapper.ProjectTopoTableMapper;
import com.creditease.dbus.domain.mapper.ProjectTopoTableMetaVersionMapper;
import com.creditease.dbus.domain.model.ProjectTopoTable;
import com.creditease.dbus.domain.model.ProjectTopoTableEncodeOutputColumns;
import com.creditease.dbus.domain.model.ProjectTopoTableMetaVersion;
import com.creditease.dbus.domain.model.TableStatus;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static com.creditease.dbus.constant.KeeperConstants.GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS;


/**
 * Created with IntelliJ IDEA
 * Description:
 * User: 王少楠
 * Date: 2018-04-18
 * Time: 下午2:01
 */
@Service
public class ProjectTableService {

    @Autowired
    private ProjectResourceMapper resourceMapper;

    @Autowired
    private ProjectTopoTableMapper projectTopoTableMapper;

    @Autowired
    private ProjectTopoTableEncodeOutputColumnsMapper encodeColumnMapper;

    @Autowired
    private ProjectMapper projectMapper;

    @Autowired
    private IZkService zkService;

    @Autowired
    private ProjectEncodeHintMapper encodeHintMapper;

    @Autowired
    private ProjectTopoTableMetaVersionMapper tableMetaVersionMapper;

    private static Logger logger = LoggerFactory.getLogger(ProjectTableService.class);

    public static final int FOLLOW_SOURCE = 0; //输出列，跟随源端的变化而变化
    public static final int FIX_COLUMN = 1; //固定列输出

    public static final int TABLE_EXIST = -1; // 插入table时，table已存在
    public static final int TABLE_NOT_FOUND = -1; //更新或删除table时,table不存在

    public static final int TABLE_IS_RUNNING = -2; //删除table时，running状态的不可以

    public static final int SCHEMA_CHANGE_TRUE = 1; //表结构变更标志0:未变更,1:变更
    public static final int SCHEMA_CHANGE_FALSE = 0;

    /*
     * 0，源端脱敏；
     1，admin脱敏；
     2，自定义脱敏，用户添加的列的脱敏信息； //前端判断
     3，无，表示没有脱敏信息。   //前端判断
     *
     */
    public static final int ENCODESOURCE_TYPE_SOURCE = 0;
    public static final int ENCODESOURCE_TYPE_ADMIN = 1;
    public static final int ENCODESOURCE_TYPE_USER = 2;
    public static final int ENCODESOURCE_TYPE_NONE = 3;

    public PageInfo<Map<String,Object>> queryResource(String dsName,
                                                      String schemaName,
                                                      String tableName,
                                                      Integer pageNum,
                                                      Integer pageSize,
                                                      Integer projectId,
                                                      Integer topoId,
                                                      Integer hasDbaEncode){
        Map<String,Object> param = new HashedMap();
        param.put("dsName",dsName == null? dsName : dsName.trim());
        param.put("schemaName",schemaName == null? schemaName: schemaName.trim());
        param.put("tableName",tableName == null ? tableName : tableName.trim());
        param.put("projectId",projectId);
        param.put("topoId",topoId);
        param.put("hasDbaEncode",hasDbaEncode);


        PageHelper.startPage(pageNum,pageSize);
        //查询到project下所有resource信息
        List<Map<String,Object>> resources = resourceMapper.searchTableResource(param);
        /*List<Map<String,Object>> resources = resourceMapper.search(param);

        //查询到project下，已添加到topo上的table
        Map<String,Object> tableParam = new HashedMap();
        tableParam.put("projectId",projectId);
        tableParam.put("topoId",topoId);
        List<Map<String,Object>> tables =projectTopoTableMapper.searchTable(tableParam);

        //从所有的resource中删除，已经添加的table，表示该topo下能添加的resource。
        Iterator<Map<String,Object>> resourceIterator =resources.iterator();
        while(resourceIterator.hasNext()){
            Map<String,Object> resource = resourceIterator.next();
            long stableId = (long) resource.get("tableId");
            //从resouce集合中删除已添加的
            for(Map<String,Object> table: tables){
                if((long)table.get("sourcetableId") == stableId){
                    resourceIterator.remove();
                    continue;
                }
            }
            resource.remove("projectName");
            resource.remove("version");
            resource.remove("createTime");
        }*/
        return new PageInfo(resources);
    }

    public PageInfo<Map<String, Object>> queryTable(String dsName,
                                                    String schemaName,
                                                    String tableName,
                                                    Integer pageNum,
                                                    Integer pageSize,
                                                    Integer projectId,
                                                    Integer topoId) {
        //1.根绝条件查询topoTable列表
        Map<String, Object> param = new HashedMap();
        param.put("dsName", dsName == null ? dsName : dsName.trim());
        param.put("schemaName", schemaName == null ? schemaName : schemaName.trim());
        param.put("tableName", tableName == null ? tableName : tableName.trim());
        param.put("projectId", projectId);
        param.put("topoId", topoId);
        //放在查询前，且只有紧跟在方法后的第一个Mybatis的查询（Select）方法会被分页
        PageHelper.startPage(pageNum, pageSize);
        long start = System.currentTimeMillis();
        List<Map<String, Object>> tables = projectTopoTableMapper.searchTable(param);
        long end = System.currentTimeMillis();
        logger.info("search project topo table cost time {}", end - start);
        //tableId,projectId,projectName,projectDisplayName,topoId,sourcetableId,dsType,dsName,schemaName,tableName,
        //tableNameAlias,physicalTableRegex,version,topoName,topoStatus,inputTopic,outputTopic,status,outputType,schemaChangeFlag,description,ifFullpull
        ArrayList<Long> sourceTableIds = new ArrayList<>();
        ArrayList<Long> tableIds = new ArrayList<>();
        for (Map<String, Object> table : tables) {
            sourceTableIds.add((Long) table.get("sourcetableId"));
            tableIds.add((Long) table.get("tableId"));
        }
        //2.查询是否有dba脱敏
        long start1 = System.currentTimeMillis();
        if(sourceTableIds.size()!=0){
            List<Long> dbaEncodeTable = projectTopoTableMapper.getDbaEncodeTable(sourceTableIds);
            for (Map<String, Object> table : tables) {
                if (dbaEncodeTable.contains(table.get("sourcetableId"))) {
                    table.put("hasDbaEncode", 0);
                }
            }
        }
        long end1 = System.currentTimeMillis();
        logger.info("search dba encode table cost time {}", end1 - start1);

        //3.查询是否使用dba脱敏
        if(tableIds.size()!=0) {
            List<Long> dbaEncodeProjectTable = projectTopoTableMapper.getDbaEncodeProjectTable(tableIds);
            for (Map<String, Object> table : tables) {
                if (dbaEncodeProjectTable.contains(table.get("tableId"))) {
                    table.put("useDbaEncode", 0);
                }
            }
        }
        long end2 = System.currentTimeMillis();
        logger.info("search dba encode project topo table cost time {}", end2 - end1);
        return new PageInfo(tables);
    }

    public List<Map<String, Object>> queryTable(String dsName, String schemaName, String tableName,
                                                    Integer projectId, Integer topoId) {
        Map<String,Object> param = new HashedMap();
        param.put("dsName",dsName==null?dsName:dsName.trim());
        param.put("schemaName",schemaName == null? schemaName:schemaName.trim());
        param.put("tableName",tableName == null? tableName:tableName.trim());
        param.put("projectId",projectId);
        param.put("topoId",topoId);
        return projectTopoTableMapper.searchProjectTable(param);
    }

    public List<Map<String,Object>> getProjectNames(){
        return projectTopoTableMapper.getProjectNames();
    }

    public List<Map<String,Object>> getTopologyNames(Integer projectId){
        return projectTopoTableMapper.getTopologyNames(projectId);
    }

    public List<Map<String,Object>> getProjectTopologies(Integer projectId){
        return projectTopoTableMapper.getProjectTopologies(projectId);
    }

    public List<Map<String,Object>> getDSNames(Integer projectId){
        return projectTopoTableMapper.getDSNames(projectId);
    }

    public int deleteByTableId(int topoTableId,String topoStatus){
        ProjectTopoTable table2Del = projectTopoTableMapper.selectByPrimaryKey(topoTableId);
        if (table2Del == null) {
            return MessageCode.TABLE_NOT_EXISTS;
        }
        int projectId = table2Del.getProjectId();
        int topoId = table2Del.getTopoId();
        int sourceTableId = table2Del.getTableId();
        if (StringUtils.equalsIgnoreCase(topoStatus, TableStatus.RUNNING.getValue()) || StringUtils.equalsIgnoreCase(topoStatus, TableStatus.CHANGED.getValue())) {
            if (!StringUtils.equalsIgnoreCase(table2Del.getStatus(), TableStatus.STOPPED.getValue())) {
                return MessageCode.TABLE_IS_RUNNING;
            }
        }
        //删除table信息
        projectTopoTableMapper.deleteByPrimaryKey(topoTableId);
        //删除column信息
        encodeColumnMapper.deleteByTopoTableId(topoTableId);
        //删除meta_version信息
        tableMetaVersionMapper.deleteByNonPrimaryKey(projectId, topoId, sourceTableId, null);
        logger.info("delete topo table success. projectId;{},topoId:{},tableId:{}", projectId, topoId, sourceTableId);
        return 0;
    }

    public int deleteColumnByTableId(int topoTableId){
        //删除column信息
        return encodeColumnMapper.deleteByTopoTableId(topoTableId);
    }

    public int insert(ProjectTopoTable table){
        int projectId = table.getProjectId();
        int topoId = table.getTopoId();
        int sourceTableId = table.getTableId();
        ProjectTopoTable oldeTable = findByIds(projectId,topoId,sourceTableId);
        if(oldeTable != null){
            return TABLE_EXIST;
        }else {
            projectTopoTableMapper.insert(table);
        }
        return table.getId();
    }

    public void insertColumns(List<ProjectTopoTableEncodeOutputColumns> columnsList){
        for(ProjectTopoTableEncodeOutputColumns column: columnsList){
            //根据topoTableId和fieldName判断是否存在,如果存在，则直接更新
            if(column.getProjectTopoTableId() == null || column.getFieldName() == null){
                logger.error("[insert columns error] projectTableId is null or fieldName is null. projectTableId:{},fieldName:{}",
                        column.getProjectTopoTableId(),column.getFieldName());
                continue;
            }
            ProjectTopoTableEncodeOutputColumns oldColumn = encodeColumnMapper.selectByTopoTableIdAndFieldName(
                    column.getProjectTopoTableId(),column.getFieldName());
            if(oldColumn !=null){
                column.setId(oldColumn.getId());
                logger.info("[encode] will update EncodeOutputColumns with param {}", column);
                encodeColumnMapper.updateByPrimaryKey(column);
            }else {
                logger.info("[encode] will insert EncodeOutputColumns with param {}", column);
                encodeColumnMapper.insert(column);
            }
        }
    }

    /**
     * {
     *     sink:{"sinkId":1,"ouputType":json/ums1.1","outputTopic":"test2test"},
     *     resource:{"schemaName":"xx",..,"topoId":1},
     *     "encodes":{
     *                "2066":{"outputListType":"1","encodeOutputColumns":[
     *                        {"fieldName":"a","encodeType":"type","encodeParam":"1","truncate":"1"}
     *                        {"fieldName":"b","encodeType":"type","encodeParam":"1","truncate":"1"}
     *                           ]
     *                    }
     *      }
     */
    /**
     * 根据tableId,返回table信息
     * @param projectTableId
     * @return table信息 or null(Exception)；  column的信息没有disable字段，在manager中根据encodehint来判断
     */
    public Map<String,Object> queryById(int projectId,int projectTableId) throws Exception{

        //返回结果
        Map<String, Object> result = new HashedMap();
        //获取table基本信息
        Map<String, Object> tableMsg = projectTopoTableMapper.selectByTableId(projectTableId);
        if(tableMsg ==null || ((Long)tableMsg.get("projectId"))!=projectId){
            logger.error("[query table by id] Table not found. projectTableId:{},",projectTableId);
            throw new Exception(String.valueOf(MessageCode.TABLE_NOT_EXISTS));
        }
        try {
            int sinkId = (int) tableMsg.get("sinkId");
            String outputType = (String) tableMsg.get("outputType");
            tableMsg.remove("outputType");
            String outputTopic = (String) tableMsg.get("outputTopic");
            tableMsg.remove("outputTopic");
            int outputListType = (int) tableMsg.get("outputListType");
            tableMsg.remove("outputListType");

            int sourceTableId = ((Long)tableMsg.get("sourceTableId")).intValue();
            tableMsg.remove("sourceTableId");

            //返回的tableId是topoTableId,前端需要的tableId是sourceTableId
            tableMsg.put("projectTableId",sourceTableId);

            int topoId = ((Long)tableMsg.get("topoId")).intValue();
            int version = (int) tableMsg.get("version");

            //新的逻辑-获取所有的脱敏信息：dbaEncode+adminEncode+userEncode
            List<Map<String,Object>> adminUserEncodeColumns = getEncodeColumns(projectTableId);

            //源端column信息
            List<Map<String, Object>> sourceEncodeColumns = projectMapper.selectColumns(sourceTableId);

            //结果列
            List<Map<String, Object>> resultColumns = sourceEncodeColumns;

            /* 如果贴源输出，输出的列就是从源端获取的；如果是固定列，则需要根据table_meta来判断，需要输出的列。
            * 然后将设置的脱敏(admin+user)信息加到输出列上,---->脱敏列和源端列取并集---->脱敏输出列
            * */
            if(outputListType == FIX_COLUMN) {
                resultColumns = new LinkedList<>();
                //输出列以table_meta为准
                List<ProjectTopoTableMetaVersion> taleMetas = tableMetaVersionMapper.selectByNonPrimaryKey(projectId, topoId, sourceTableId, version);
                for (ProjectTopoTableMetaVersion metaVersion : taleMetas) {
                    Map<String, Object> outputColumn = new HashedMap();

                    String outputColumnName = metaVersion.getColumnName();
                    outputColumn.put("columnName", outputColumnName);
                    //以meta表为准
                    outputColumn.put("dataType",metaVersion.getDataType());
                    outputColumn.put("dataLength",metaVersion.getDataLength());
                    outputColumn.put("dataScale",metaVersion.getDataScale());
                    outputColumn.put("dataPrecision",metaVersion.getDataPrecision());
                    outputColumn.put("schemaChangeFlag", metaVersion.getSchemaChangeFlag());
                    outputColumn.put("schemaChangeComment", metaVersion.getSchemaChangeComment());

                    resultColumns.add(outputColumn);

                    // 将源端的脱敏信息添加到，输出结果
                    Iterator<Map<String, Object>> iterator = sourceEncodeColumns.iterator();
                    while (iterator.hasNext()) {
                        Map<String, Object> sourceEncodeColumn = iterator.next();
                        String columnRowName = sourceEncodeColumn.get("columnName").toString();
                        if (sourceEncodeColumn.get("encodeType") != null) {
                            //默认是源端脱敏
                            sourceEncodeColumn.put("encodeSource", ENCODESOURCE_TYPE_SOURCE);
                        }
                        if (StringUtils.equals(outputColumnName, columnRowName)) {
                            outputColumn.putAll(sourceEncodeColumn);
                        }
                    }
                }
            }
            //将所有脱敏信息和 disable字段 添加到输出列
            for(Map<String, Object> resultColumn: resultColumns) {
                //将非源端脱敏信息添加到输出列
                for(Map<String,Object> adminUserEncodeColumn: adminUserEncodeColumns) {
                    if(StringUtils.equals(resultColumn.get("columnName").toString(),
                            adminUserEncodeColumn.get("columnName").toString())) {
                        resultColumn.put("encodeType",adminUserEncodeColumn.get("encodeType"));
                        resultColumn.put("encodeParam",adminUserEncodeColumn.get("encodeParam"));
                        resultColumn.put("truncate",adminUserEncodeColumn.get("truncate"));
                        resultColumn.put("encodeSource",adminUserEncodeColumn.get("encodeSource"));
                        resultColumn.put("encodePluginId",adminUserEncodeColumn.get("encodePluginId"));
                        resultColumn.put("schemaChangeFlag",adminUserEncodeColumn.get("schemaChangeFlag"));
                        resultColumn.put("schemaChangeComment",adminUserEncodeColumn.get("schemaChangeComment"));
                        resultColumn.put("specialApprove",adminUserEncodeColumn.get("specialApprove"));

                        //前端需要改字段作为column的id
                        if(resultColumn.get("cid") == null){
                            resultColumn.put("cid",resultColumn.get("columnName").toString()+System.currentTimeMillis());
                        }
                        break;
                    }
                }

                /*根据脱敏信息， 判断disable字段*/
                Integer encodeSource = resultColumn.get("encodeSource") == null ? null:(int) resultColumn.get("encodeSource");
                if(encodeSource == null){//没有脱敏信息或者是贴源的源端脱敏时
                    if (resultColumn.get("encodeType") != null) {
                        resultColumn.put("encodeSource", ENCODESOURCE_TYPE_SOURCE);
                        resultColumn.put("disable",true);
                    }else {
                        resultColumn.put("encodeSource", ENCODESOURCE_TYPE_NONE);
                        resultColumn.put("disable",false);
                    }

                    continue;
                }
                //源端脱敏
                if(encodeSource == ENCODESOURCE_TYPE_SOURCE){
                    resultColumn.put("disable",true);
                    continue;
                }else if(encodeSource == ENCODESOURCE_TYPE_ADMIN){
                    String encodeType = resultColumn.get("encodeType").toString();
                    if(StringUtils.isNotEmpty(encodeType)&&StringUtils.equals(encodeType.toLowerCase(),"none")){
                        // admin添加了encodeType字段，但字段为none说明用户添加该列的时候必须指定具体值
                        resultColumn.put("disable",false);
                    }else {
                        resultColumn.put("disable",true);
                    }
                }else {//用户自定义的
                    resultColumn.put("disable",false);
                }
            }


            /* 拼接结果 */
            result.put("resource",tableMsg);
            //构造sink
            Map<String, Object> sink = new HashedMap();
            sink.put("sinkId",sinkId);
            sink.put("outputType",outputType);
            sink.put("outputTopic",outputTopic);
            result.put("sink",sink);

            //构造encodes
            Map<String, Object> encodes = new HashedMap();
            Map<String,Object> encode =new HashMap<>();
            encode.put("outputListType",outputListType);
            encode.put("encodeOutputColumns",resultColumns);
            encodes.put(String.valueOf(sourceTableId),encode);
            result.put("encodes",encodes);

            return result;

        }catch (Exception e){
            logger.error("[query table by id] Catch exception. projectId:{},projectTableId:{}, Exception:{}",projectId,projectTableId,e);
            throw new Exception(String.valueOf(MessageCode.TABLE_DATA_FORMAT_ERROR));
            //return null;
        }
    }

    public List<Map<String,Object>> getEncodeColumns(Integer topoTableId){
        return encodeColumnMapper.selectByTableId(topoTableId);
    }

    public int updateTable(ProjectTopoTable table){
        ProjectTopoTable oldTopoTable =findTableById(table.getId());
        if(oldTopoTable == null){
            logger.info("[update projectTopoTable] table not exists. tableId:{}",table.getId());
            return TABLE_NOT_FOUND;
        }
        //如果oldTopoTable是的状态running，则更改为changed.(stopped不更改，changed保持)
        if(TableStatus.RUNNING.getValue().equals(oldTopoTable.getStatus())){
            table.setStatus(TableStatus.CHANGED.getValue());
        }
        //每次更新，meta_ver字段值 +1，。更新后，meta_version表的数据，根据topoTable表的字段再更新。
        int newMetaVer = oldTopoTable.getMetaVer()+1;
        table.setMetaVer(newMetaVer);
        return projectTopoTableMapper.updateByPrimaryKey(table);
    }

    public void updateColumns(List<ProjectTopoTableEncodeOutputColumns> columnList){
        for(ProjectTopoTableEncodeOutputColumns column: columnList){
            //之前存在的列，有id，直接更新。
            if(column.getId() !=null){
                encodeColumnMapper.updateByPrimaryKey(column);
            }else {
                encodeColumnMapper.insert(column);
            }
        }
    }

    public List<Map<String,String>> getTopicOffsets(String topic){
        KafkaConsumer<String, String> consumer = null;
        try {
            Properties consumerProps = zkService.getProperties(KeeperConstants.KEEPER_CONSUMER_CONF);
            consumerProps.setProperty("client.id", "");
            consumerProps.setProperty("group.id", "topic.offsets.reader.temp");
            Properties globalConf = zkService.getProperties(KeeperConstants.GLOBAL_CONF);
            consumerProps.setProperty(GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS, globalConf.getProperty(GLOBAL_CONF_KEY_BOOTSTRAP_SERVERS));
            List<Map<String, String>> topicMsg = new ArrayList<>();
            // 新建consumer
            consumer = new KafkaConsumer<String, String>(consumerProps);
            /*//订阅topic(订阅所有partition，否则会抛出"You can only check the position for partitions assigned to this consumer.")
            consumer.subscribe(Arrays.asList(topic));*/
            // 获取topic的partition列表
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);

            // 获取每个partition信息
            for (PartitionInfo partitionInfo : partitionInfos) {
                int partition = partitionInfo.partition();
                TopicPartition topicPartition = new TopicPartition(topic, partition);
                consumer.assign(Arrays.asList(topicPartition));

                consumer.seekToEnd(consumer.assignment());
                //下一次拉取位置
                long nextFetchOffset = consumer.position(topicPartition);

                consumer.seekToBeginning(consumer.assignment());
                long headOffset = consumer.position(topicPartition);

                Map<String, String> partitionMsg = new HashedMap();
                partitionMsg.put("topic", topic);
                partitionMsg.put("partition", String.valueOf(partition));
                partitionMsg.put("latestOffset", String.valueOf(nextFetchOffset));
                partitionMsg.put("headOffset", String.valueOf(headOffset));
                topicMsg.add(partitionMsg);

            }

            return topicMsg;
        }catch (Exception e){
            logger.error("[table topic offset] Error encountered while getting topic messages. topic:{}",topic );
            return null;
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }

    public List<String> getTableNamesByTopic(String topic,int tableId){
        Map<String,Object> params = new HashedMap();
        params.put("tableId",tableId);
        params.put("topic",topic);
        return projectTopoTableMapper.selectNameByTopic(params);
    }

    public ProjectTopoTable findTableById(Integer tableId){
        return projectTopoTableMapper.selectByPrimaryKey(tableId);
    }

    public Map<Integer,Set<String>> getExistedTopicsByProjectId(Long projectId){
        Map<Integer,Set<String>> topicsGroupedBySink = new HashedMap();
        List<Map<String,Object>> resultMap = projectTopoTableMapper.getExistedTopicsByProjectId(projectId);
        for (Map<String,Object> topicDataMap:resultMap) {
            Integer sinkId =(Integer)topicDataMap.get("sink_id");
            String outPutTopic = (String)topicDataMap.get("output_topic");
            if(topicsGroupedBySink.containsKey(sinkId)){
                topicsGroupedBySink.get(sinkId).add(outPutTopic);
            }else {
                Set<String> topicsOfSink = new HashSet<>();
                topicsOfSink.add(outPutTopic);
                topicsGroupedBySink.put(sinkId,topicsOfSink);
            }
        }
        return topicsGroupedBySink;
    }

    /**
     * 根据ProjectId,topoId和tableId(source table id)判断，ProjectTable是否已存在
     */
    private ProjectTopoTable findByIds(int projectId,int topoId, int tableId){
        return projectTopoTableMapper.selectByPIdTopoIdTableId(projectId,topoId,tableId);
    }
    /**
     * 插入meta_version信息
     * meta_version信息直插入，不更新，不删除
     */
    public void insertOrUpdateMetaVersions(List<ProjectTopoTableMetaVersion> metaVersions){
        if(metaVersions == null || metaVersions.size() < 1){
            logger.error("[insert meta versions]: input message is empty");
            throw new IllegalArgumentException("参数有误");
        }
        //list中所有的数据，projectId,topoId,和tableId都是一样的。
        ProjectTopoTableMetaVersion firstOne = metaVersions.get(0);
        int projectId = firstOne.getProjectId();
        int topoId = firstOne.getTopoId();
        int tableId = firstOne.getTableId();
        //使用projectTable的version值为column的version字段赋值
        ProjectTopoTable projectTable = findByIds(projectId, topoId, tableId);
        Iterator<ProjectTopoTableMetaVersion> iterator = metaVersions.iterator();
        while (iterator.hasNext()){
            ProjectTopoTableMetaVersion newOne = iterator.next();
            newOne.setVersion(projectTable.getMetaVer());

            //直接插入新数据，保留旧数据
            tableMetaVersionMapper.insert(newOne);
        }

        /*先获取已有的数据，如果存在已有数据，需要对旧数据进行处理：更新或删除 */
        /*
        List<ProjectTopoTableMetaVersion> oldMetaVersions = tableMetaVersionMapper
                .selectByNonPrimaryKey(projectId,topoId,tableId,null);

        Iterator<ProjectTopoTableMetaVersion> oldIterator = oldMetaVersions.iterator();
        while (oldIterator.hasNext()){
            //对旧数据进行处理：更新
            ProjectTopoTableMetaVersion oldOne = oldIterator.next();
            Iterator<ProjectTopoTableMetaVersion> newIterator = metaVersions.iterator();
            while(newIterator.hasNext()){
                //根据projectTable的meta版本更新当前meta版本
                ProjectTopoTableMetaVersion newOne = newIterator.next();
                ProjectTopoTable projectTopoTable = findByIds(projectId, topoId, tableId);
                newOne.setVersion(projectTopoTable.getMetaVer());

                //projectId,topoId,tableId都是一致的，比较columnName即可
                if(StringUtils.equals(newOne.getColumnName(), oldOne.getColumnName())){
                    newOne.setId(oldOne.getId());
                    tableMetaVersionMapper.updateByPrimaryKey(newOne);

                    //更新完毕，剔除更新的公共部分
                    oldIterator.remove();
                    newIterator.remove();
                    break;
                }
            }
        }
        //对剩余旧数据处理：删除
        for(ProjectTopoTableMetaVersion oldOne : oldMetaVersions){
            tableMetaVersionMapper.deleteByPrimaryKey(oldOne.getId());
        }

        //对剩余新数据：新增
        for(ProjectTopoTableMetaVersion newOne: metaVersions){
            tableMetaVersionMapper.insert(newOne);
        }*/
    }

    /**
     *更新meta_version信息
     */
    public void updateMetaVersions(List<ProjectTopoTableMetaVersion> metaVersions){
        for(ProjectTopoTableMetaVersion metaVersion: metaVersions){
            int projectId = metaVersion.getProjectId();
            int topoId = metaVersion.getTopoId();
            int sourceTableId = metaVersion.getTableId();
            String columnName = metaVersion.getColumnName();
            //判断是否存在,如果存在，则直接更新

                //todo metaVersion.setId(oldOne.getId());
                //meta_version的增加操作交给update table时处理。这里更新的时候，直接从table里取值
                ProjectTopoTable projectTopoTable = findByIds(projectId, topoId, sourceTableId);
                metaVersion.setVersion(projectTopoTable.getMetaVer());
                tableMetaVersionMapper.updateByPrimaryKey(metaVersion);
        }
    }

    public int countByDsId(Integer dsId) {
        return projectTopoTableMapper.countByDsId(dsId);
    }

    public int countByTableId(Integer tableId) {
        return projectTopoTableMapper.countByTableId(tableId);
    }

    public int countBySchemaId(Integer schemaId) {
        return projectTopoTableMapper.countBySchemaId(schemaId);
    }

    public List<ProjectTopoTable> getTopoTablesByUserId(Integer userId) {
        return projectTopoTableMapper.getTopoTablesByUserId(userId);
    }

    public List<Map<String, Object>> getAllResourcesByQuery(String dsName, String schemaName, String tableName,
                                                            Integer projectId, Integer topoId) {
        Map<String,Object> param = new HashedMap();
        param.put("dsName",dsName == null? dsName : dsName.trim());
        param.put("schemaName",schemaName == null? schemaName: schemaName.trim());
        param.put("tableName",tableName == null ? tableName : tableName.trim());
        param.put("projectId",projectId);
        param.put("topoId",topoId);
        return resourceMapper.searchTableResource(param);
    }

    public int underOtherTopologyTableCountInSameProject(Integer projectId, Integer tableId, Integer topoId) {
        return projectTopoTableMapper.underOtherTopologyTableCountInSameProject(projectId, tableId, topoId);
    }

    /**
     * 根据topoTableId获取
     * ds_name,schema_name,table_name, project_name,topo_name
     */
    public Map<String, Object> getNamesByTopoTableId(Integer topoTableId) {
        return projectTopoTableMapper.getNamesByTopoTableId(topoTableId);
    }

    public List<Map<String,Object>> getTopoTablesByIds(List<Integer> topoTableIds) {
        return projectTopoTableMapper.getTopoTablesByIds(topoTableIds);
    }

    public void updateStatusByTopoTableIds(String status,List<Integer> topoTableIds) {
        projectTopoTableMapper.updateStatusByTopoTableIds(status,topoTableIds);
    }

    public List<Map<String, Object>> queryTable(List<Integer> topoTableIds) {
        return projectTopoTableMapper.searchTableByTopoTableIds(topoTableIds);
    }
}
