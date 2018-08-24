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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.base.com.creditease.dbus.utils.RequestSender;
import com.creditease.dbus.bean.AllEncodersBean;
import com.creditease.dbus.bean.ProjectTableOffsetBean;
import com.creditease.dbus.bean.ProjectTopoTableEncodeOutputColumnsBean;
import com.creditease.dbus.bean.TableBean;
import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.IZkService;
import com.creditease.dbus.constant.KeeperConstants;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.constant.ServiceNames;
import com.creditease.dbus.domain.model.*;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.fasterxml.jackson.core.type.TypeReference;
import kafka.utils.ZkUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import scala.collection.JavaConversions;

import java.text.MessageFormat;
import java.util.*;

/**
 * Created with IntelliJ IDEA
 * Description:
 * User: 王少楠
 * Date: 2018-04-18
 * Time: 下午6:31
 */
@Service
public class ProjectTableService {
    @Autowired
    private RequestSender sender;

    @Autowired
    private TableService tableService;

    @Autowired
    private FullPullService fullPullService;

    @Autowired
    private ProjectService projectService;

    @Autowired
    private ProjectTopologyService projectTopologyService;

    @Autowired
    private ToolSetService toolSetService;

    @Autowired
    private IZkService zkService;

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

    public static final int FOLLOW_SOURCE = 0; //输出列，跟随源端的变化而变化
    public static final int FIX_COLUMN = 1; //固定列输出

    public static final int META_VERTION_INIT = 0; //meta_ver字段初始值

    public static final byte SHCEMA_CHANGE_FLAG_FALSE = 0; //0表示正常

    private static Logger logger = LoggerFactory.getLogger(ProjectTableService.class);

    public ResultEntity queryTable(String queryString) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectTable/search-table", queryString);
        //topoStatus不能只根据数据库的值
        Map<String, Object> projectTables = result.getBody().getPayload(new TypeReference<LinkedHashMap<String, Object>>() {});
        if (projectTables != null) {
            List<Map<String, Object>> data = (List<Map<String, Object>>) projectTables.get("list");
            for (Map<String, Object> datum : data) {
                String status = (String) datum.get("topoStatus");
                String name = (String) datum.get("topoName");
                try {
                    datum.put("topoStatus", projectTopologyService.correctStatus(status, name));
                } catch (Exception e) {
                    logger.info("[query tables]correct topology status exception. " +
                            "projectTableId:{},topoName:{}",datum.get("tableId"),name);
                }
            }
        }
        result.getBody().setPayload(projectTables);
        return result.getBody();

    }

    public ResultEntity queryTopologyNames(String queryString){
        ResponseEntity<ResultEntity> result= sender.get(ServiceNames.KEEPER_SERVICE, "/projectTable/topology-names",queryString);
        return result.getBody();
    }

    public ResultEntity getPojectTopologies(String queryString){
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectTable/project-topologies",queryString);
        return result.getBody();
    }

    public ResultEntity queryProjectNames(){
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectTable/project-names");
        return result.getBody();
    }

    public ResultEntity queryDSNames(String queryString){
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectTable/datasource-names",queryString);
        return result.getBody();
    }

    /**
     * 查询该项目下的Resource
     * @queryString: projectId
     */
    public ResultEntity queryProjectResources(String queryString){
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectTable/search-resource",queryString);
        return result.getBody();
    }

    public ResultEntity getEncodeColumns(String queryString){
        //获取table下所有column信息,包括源端脱敏信息
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projects/columns", queryString);
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
            return result.getBody();
        List<Map<String, Object>> rowColumns= result.getBody().getPayload(new TypeReference<List<Map<String, Object>>>() {});

        //获取添加项目时，admin配置的脱敏信息
        result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectEncodeHint/select-by-pid-tid", queryString);
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
            return result.getBody();
        List<Map<String, Object>> encodeColumns = result.getBody().getPayload(new TypeReference<List<Map<String, Object>>>() {});

        for(Map<String, Object> column: rowColumns){
            int tid =(Integer)column.get("tid");
            int cid =(Integer)column.get("cid");
            //源端脱敏，保留
            if(column.get("encodeType") !=null){
                /*encodeSource字段：
                    0，源端脱敏；
                    1，admin脱敏；
                    2，自定义脱敏，用户添加的列的脱敏信息； //前端判断
                    3，无，表示没有脱敏信息。   //前端判断
                在获取column信息时返回。在添加table时，将该字段作为encodeOutputColumn的信息。
                 */
                column.put("encodeSource",ENCODESOURCE_TYPE_SOURCE);
                //disable 字段是给前端判断是否禁掉，能不能编辑用的
                column.put("disable",true);
                continue;
            }else {
                //如果admin配置了脱敏信息，就在脱敏信息加上
                for(Map<String, Object> encodeColumn: encodeColumns){
                    //将精度的两个字段赋值
                    column.put("dataPrecision",encodeColumn.get("dataPrecision"));
                    column.put("dataScale",encodeColumn.get("dataScale"));

                    int tableId =(int)encodeColumn.get("tableId");
                    int columnId =(int)encodeColumn.get("columnId");
                    if(tid==tableId && cid == columnId){
                        String encodeType = (String) encodeColumn.get("encodeType");
                        column.put("encodeType",encodeType);
                        column.put("encodeParam",encodeColumn.get("encodeParam"));
                        column.put("truncate",encodeColumn.get("truncate"));
                        column.put("encodeSource",ENCODESOURCE_TYPE_ADMIN);
                        column.put("encodePluginId",encodeColumn.get("encodePluginId"));
                        if(StringUtils.isNotEmpty(encodeType)&&StringUtils.equals(encodeType.toLowerCase(),"none")){
                            // admin添加了encodeType字段，但字段为none说明用户添加该列的时候必须指定具体值
                            column.put("disable",false);
                        }else {
                            column.put("disable",true);
                        }
                        break;
                    }else{
                        //源端和admin都没有配置脱敏信息
                        column.put("disable",false);
                    }
                }
            }

        }

        result.getBody().setPayload(rowColumns);
        return result.getBody();
    }

    public ResultEntity getColumns(String queryString){
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projects/columns", queryString);
        return result.getBody();
    }


    public ResultEntity getEncodeOutputColumns(Integer topoTableId){
        ResponseEntity<ResultEntity>  result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectTable/encode-columns/{tableId}", topoTableId);
        return result.getBody();
    }

    /**
     * 获取项目使用的 topicList
     */
    public ResultEntity getSinkTopics(long projectId) {
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectSink/getExistedTopicsByProjectId/{projectId}", projectId);
        return result.getBody();
        /**
         * Deprecated.
         * 原来的设计：从kafka读取当前sink已存在的topics。
         * 后来的设计：涉及到给租户授权topics。要求租户自己明确建立topic。
         * 用户只拥有访问自己创建的topic的权力。
         * 为防设计反复，旧代码先保留。

         if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
         return result.getBody();
         Sink tableSink =result.getBody().getPayload(new TypeReference<Sink>() {});
         String url = tableSink.getUrl();

         Map<String, List<PartitionInfo>> topics;

         Properties props = new Properties();
         props.put("bootstrap.servers", url);
         props.put("request.timeout.ms","5000");
         props.put("enable.auto.commit", "false");
         props.put("auto.commit.interval.ms", "2000");
         props.put("session.timeout.ms", "3100");
         props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
         props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

         KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);
         try {
         topics = consumer.listTopics();
         }catch (TimeoutException e){
         logger.info("get topic list timeout! {}",e);
         return null;
         }catch (Exception e){
         logger.info("get topic exception,{}",e);
         return null;
         }
         consumer.close();
         result.getBody().setPayload(new ArrayList(topics.keySet()));
         return result.getBody();
         * */

    }

    public ResultEntity getProjectSinks(int projectId){
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectSink/select-by-project-id/{id}",projectId);
        return result.getBody();
    }

    /**
     *  {
     *      "projectId":"1",
     *      "topoId":"1",
     *      "outputTopic":"dbus",
     *      "outputType":"json",
     *      "sinkId":"1",
     *      "newTpoic":true
     *      "encodes":{
     *                "1":{"outputListType":"1","encodeOutputColumns":[
     *                        {"fieldName":"a","encodeType":"type","encodeParam":"1","truncate":"1"}//encoudeSource 和 desc？
     *                        {"fieldName":"b","encodeType":"type","encodeParam":"1","truncate":"1",}]
     *                        },
     *
     *                "2":{"outputListType":"1","encodeOutputColumns":[
     *                        {"fieldName":"a","encodeType":"type","encodeParam":"1","truncate":"1"}
     *                        {"fieldName":"b","encodeType":"type","encodeParam":"1","truncate":"1"}]
     *                        }
     *                }
     *  }
     */
    public ResultEntity addTable(TableBean table) throws Exception{
        ResponseEntity<ResultEntity> result = null;
        //获取topoTable的一些公共信息
        int projectId =table.getProjectId();
        int topoId = table.getTopoId();
        int sinkId = table.getSinkId();
        String outputType = table.getOutputType();
        String outputTopic = table.getOutputTopic();
        Map<Integer,ProjectTopoTableEncodeOutputColumnsBean> encodes = table.getEncodes();
        if(encodes == null){
            logger.error("[add table] Input param error: lack encodes. TableBean:{}",table);
            return null;
        }
        List<ProjectTopoTableEncodeOutputColumns> encodeOutputColumns;

        for(Map.Entry<Integer,ProjectTopoTableEncodeOutputColumnsBean> encode : encodes.entrySet()){
            ProjectTopoTable topoTable = new ProjectTopoTable();
            topoTable.setProjectId(projectId);
            topoTable.setTableId(encode.getKey());
            topoTable.setTopoId(topoId);
            topoTable.setStatus(TableStatus.STOPPED.getValue()); //默认stop
            topoTable.setOutputTopic(outputTopic);
            topoTable.setUpdateTime(new Date());
            topoTable.setOutputType(outputType);
            topoTable.setSinkId(sinkId);
            ProjectTopoTableEncodeOutputColumnsBean columnsBean = encode.getValue();
            topoTable.setOutputListType(columnsBean.getOutputListType());
            topoTable.setMetaVer(META_VERTION_INIT);

            encodeOutputColumns=columnsBean.getEncodeOutputColumns();
            if(encodeOutputColumns == null){
                logger.error("[add table] Input param error: lack encodeOutputColumns. TableBean:{}",table);
                return null;
            }
            /* 固定列的时候，构造meta_version信息*/
            List<ProjectTopoTableMetaVersion> metaVersions = null;
            if(columnsBean.getOutputListType() == FIX_COLUMN) {
                metaVersions= new ArrayList<>(encodeOutputColumns.size());
            }

            //插入topoTable的信息
            result = sender.post(ServiceNames.KEEPER_SERVICE, "/projectTable/insert",topoTable);
            if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
                return result.getBody();


            //获取topo_table_id
            int topoTableId =result.getBody().getPayload(Integer.class);

            Iterator<ProjectTopoTableEncodeOutputColumns> iterator=encodeOutputColumns.iterator();
            //修改TopoTableEncodeOutputColumns 信息
            while(iterator.hasNext()){
                ProjectTopoTableEncodeOutputColumns column  = iterator.next();
                column.setProjectTopoTableId(topoTableId);
                column.setUpdateTime(new Date());
                //固定列的时候，才需要对meta_version进行操作。源端脱敏的也需要存储
                if(columnsBean.getOutputListType() == FIX_COLUMN) {
                    ProjectTopoTableMetaVersion metaVersion = new ProjectTopoTableMetaVersion();
                    metaVersion.setProjectId(projectId);
                    metaVersion.setTopoId(topoId);
                    metaVersion.setTableId(topoTable.getTableId());
                    metaVersion.setVersion(META_VERTION_INIT); //第一次添加，默认值
                    metaVersion.setColumnName(column.getFieldName());
                    metaVersion.setDataType(column.getFieldType());
                    metaVersion.setDataLength(column.getDataLength());
                    metaVersion.setUpdateTime(column.getUpdateTime());
                    metaVersion.setDataPrecision(column.getDataPrecision());
                    metaVersion.setDataScale(column.getDataScale());
                    metaVersions.add(metaVersion);
                }


                //encode_column只存储admin和user定义的脱敏信息
                if(column.getEncodeSource() ==ENCODESOURCE_TYPE_SOURCE ||
                        column.getEncodeSource() ==ENCODESOURCE_TYPE_NONE){
                    iterator.remove();
                }
            }

            //插入或更新column信息
            result = sender.post(ServiceNames.KEEPER_SERVICE, "/projectTable/insertColumns",encodeOutputColumns);
            if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
                return result.getBody();

            //选择固定列输出时，需要插入或更新和 meta_ver信息
            if(columnsBean.getOutputListType() == FIX_COLUMN) {
                result = sender.post(ServiceNames.KEEPER_SERVICE, "/projectTable/meta-versions", metaVersions);
                if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
                    return result.getBody();
            }

        }

        return result.getBody();

    }

    public ResultEntity getTableMessage(Integer projectId,Integer topoTableId){
        //sender.get(ServiceNames.KEEPER_SERVICE, "/project-topos/in-topics/{0}/{1}", StringUtils.EMPTY, projectId, topicId);
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectTable/select/{0}/{1}",projectId,topoTableId);
        return result.getBody();
    }


    /**
     * 更新table信息:
        table的基本信息和topo不能更改，
        只能更改：sinkId和outputTopic 以及outputListType(是否贴源输出)
     * @param tableBean
        {
     *      "id":talbleId
            "projectId":"1",
     *      "outputTopic":"dbus",
     *      "outputType":"json",
     *      "sinkId":"1",
     *      "newTpoic":true
     *      "encodes":{
     *                "resourcetableId":{"outputListType":"1","encodeOutputColumns":[
     *                        {id:"id","fieldName":"a","encodeType":"type","encodeParam":"1","truncate":"1"},
     *                        {id:"id","fieldName":"b","encodeType":"type","encodeParam":"1","truncate":"1"},
                                {fieldName":"b","encodeType":"type","encodeParam":"1","truncate":"1"}
                             ]
     *                        }
     *                }
     *  }
     * @return
     */
    public ResultEntity updateTable(TableBean tableBean){
        try {
            //参数格式化校验
            checkUpdateParamLegality(tableBean);

            Integer projectTopoTableId = tableBean.getId();
            ProjectTopoTableEncodeOutputColumnsBean encodeOutputColumnsBean = null;
            Integer sourceTableId = null;
            //一个table,map中只有一个元素
            for (Map.Entry<Integer, ProjectTopoTableEncodeOutputColumnsBean> encode : tableBean.getEncodes().entrySet()) {
                encodeOutputColumnsBean = encode.getValue();
                sourceTableId = encode.getKey();
                break;
            }

            /*对传入的topic进行校验：
            *   topic需要以project_name(非project_display_name)打头
            *   检验project_name是否是本project的
            * */
            String outputTopic = tableBean.getOutputTopic();
            int projectId = tableBean.getProjectId();
            ResponseEntity<ResultEntity>  result = sender.get(ServiceNames.KEEPER_SERVICE, "/projects/select/{id}", projectId);
            if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
                return result.getBody();
            Project currentProject = result.getBody().getPayload(Project.class);
            String projectName = currentProject.getProjectName();
            String topicHead = StringUtils.substringBefore(outputTopic,".");
            if(!StringUtils.equals(projectName,topicHead)){// 不能startWith，严格匹配
                logger.info("[update project table] outputTopic:{}  is not starts with projectName:{}",
                        outputTopic,projectName);
                result.getBody().setStatus(MessageCode.TABLE_OUTPUT_TOPIC_ERROR);
                return result.getBody();
            }

            ProjectTopoTable table = new ProjectTopoTable();
            table.setId(projectTopoTableId);
            table.setTableId(sourceTableId);
            table.setTopoId(tableBean.getTopoId());
            table.setSinkId(tableBean.getSinkId());
            table.setOutputTopic(tableBean.getOutputTopic());
            table.setOutputType(tableBean.getOutputType());
            table.setOutputListType(encodeOutputColumnsBean.getOutputListType());
            table.setProjectId(tableBean.getProjectId());
            table.setSchemaChangeFlag(SHCEMA_CHANGE_FLAG_FALSE);

            /*  并非所有的update都变成changed,stopped状态的table依旧是stopped
            * 状态是否变成changed，交有Service层判断
            * table.setStatus(TableStatus.CHANGED.getValue()); */
            //update table;
            result = sender.post(ServiceNames.KEEPER_SERVICE, "/projectTable/update",table);
            if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
                return result.getBody();

            List<ProjectTopoTableEncodeOutputColumns> columns = encodeOutputColumnsBean.getEncodeOutputColumns();

             /* 构造meta_version信息*/
            List<ProjectTopoTableMetaVersion> metaVersions = null;
            if (encodeOutputColumnsBean.getOutputListType() == FIX_COLUMN) {
                metaVersions =new ArrayList<>(columns.size());
            }

            Iterator<ProjectTopoTableEncodeOutputColumns> iterator = columns.iterator();
            while(iterator.hasNext()){
                ProjectTopoTableEncodeOutputColumns column = iterator.next();
                column.setUpdateTime(new Date());
                column.setProjectTopoTableId(projectTopoTableId);
                //固定列的时候，输出。（不删除源端脱敏的列，table_meta中的列是所有的输出列）
                if (encodeOutputColumnsBean.getOutputListType() == FIX_COLUMN) {
                    ProjectTopoTableMetaVersion metaVersion = new ProjectTopoTableMetaVersion();
                    metaVersion.setProjectId(table.getProjectId());
                    metaVersion.setTopoId(table.getTopoId());
                    metaVersion.setTableId(table.getTableId());
                    metaVersion.setVersion(META_VERTION_INIT); //第一次添加，默认值
                    metaVersion.setColumnName(column.getFieldName());
                    metaVersion.setDataType(column.getFieldType());
                    metaVersion.setDataLength(column.getDataLength());
                    metaVersion.setUpdateTime(column.getUpdateTime());
                    metaVersion.setDataPrecision(column.getDataPrecision());//setUpdateTime(column.getUpdateTime());
                    metaVersion.setDataScale(column.getDataScale());
                    metaVersion.setSchemaChangeFlag(column.getSchemaChangeFlag());
                    metaVersion.setSchemaChangeComment(column.getSchemaChangeComment());
                    metaVersions.add(metaVersion);
                }
                //encode_column表中只存储admin和user脱敏信息
                if(column.getEncodeSource().intValue() == ENCODESOURCE_TYPE_SOURCE
                        || column.getEncodeSource().intValue() == ENCODESOURCE_TYPE_NONE){
                    iterator.remove();
                }
            }

            //删除column信息
            result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectTable/delete-column-by-table-id/{id}",projectTopoTableId);
            if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
                return result.getBody();

            //update columns all
            result = sender.post(ServiceNames.KEEPER_SERVICE, "/projectTable/insertColumns",columns);
            if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
                return result.getBody();

            //update meta-version
            //固定列的时候,插入(从贴源变固定列)或更新（固定列正常更新）meta_ver信息
            if (encodeOutputColumnsBean.getOutputListType() == FIX_COLUMN) {
                result = sender.post(ServiceNames.KEEPER_SERVICE, "/projectTable/meta-versions", metaVersions);
                if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
                    return result.getBody();
            }

            return result.getBody();
        }catch (NullPointerException e){
            logger.error("[update project table]: data format error! {}",e);
            ResultEntity errorEntity = new ResultEntity();
            errorEntity.setStatus(MessageCode.TABLE_PARAM_FORMAT_ERROR);
            return errorEntity;
        }catch (Exception e){
            logger.error("[update project table] catch exception! {}",e);
            ResultEntity errorEntity = new ResultEntity();
            errorEntity.setStatus(MessageCode.TABLE_PARAM_FORMAT_ERROR);
            return errorEntity;
        }

    }

    /**
     * 检验合法性
     * @param tableBean
     * @throws Exception 空指针异常，作为不合法的依据
     */
    private void checkUpdateParamLegality(TableBean tableBean) throws Exception{
        if(tableBean.getProjectId() == null || tableBean.getEncodes() == null || tableBean.getId() == null ||
                tableBean.getSinkId() == null || tableBean.getTopoId() == null ||
                StringUtils.isBlank(tableBean.getOutputTopic()) || StringUtils.isBlank(tableBean.getOutputType())){
            throw new NullPointerException();
        }
    }

    public ResultEntity getTopicOffsets(String query){
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectTable/partition-offset",query);
        return result.getBody();
    }

    public ResultEntity getAffectedTables(String query){
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectTable/affected-tables",query);
        return result.getBody();
    }

    /**
     * Start
     * @throws Exception
     * 需要发送的消息
       {
            "from": "dbus-web",
            "id": 1528856929837,

            "payload": {
             "projectTopoTableId": 70,
             "tableId": 2073,
              "dsName": "testdb",
             "schemaName": "test",
             "tableName": "user"，

             "offset": [{
              "topic": "test.topic",
              "offsetParis": "0->1357,1->3721"
             },
             {
              "topic": "test.topic1",
              "offsetParis": "begin"
             },
            {
            "topic": "test.topic1",
            "offsetParis": "end"
            }]


            },
            "type": "ROUTER_TOPOLOGY_TABLE_START",
            "timestamp": "2018-06-19 15:05:00.000"
           }
    *
    */
    public void start(List<ProjectTableOffsetBean> offsetBeans){

        if(offsetBeans == null || offsetBeans.size() < 0){
            logger.error("[start table] offsetBeans is empty.");
            throw new IllegalArgumentException("offsetBeans is empty");
        }

        //发送时信息时，需要提供的参数信息
        String tableTopic = null; //table input topic
        String messageTopic = null; //message 发送的topic
        String type = "ROUTER_TOPOLOGY_TABLE_START";
        JSONObject payload = null;

        //payload中的具体信息
        JSONArray payloadOffsetArray = new JSONArray(); //payload中需要存储的offset数组信息
        JSONObject payloadOffset = new JSONObject(); //offset数组中存储的offset信息，每个projectTopoTable

        //offset中的offsetParis信息
        StringBuffer offsetParis = new StringBuffer();

        //发送完信息，需要根据projectTopoTableId进行更新
        int projectTopoTableId = -1;

        //其实循环中只有一个元素
        for(int i=0; i<offsetBeans.size(); i++){
            ProjectTableOffsetBean offsetBean = offsetBeans.get(i);
            //防止重复赋值，只取第一个offset赋值
            if(i == 0) {
                projectTopoTableId = offsetBean.getTableId();
                //获取projectTopoTable信息
                ProjectTopoTable projectTopoTable = getTableById(projectTopoTableId);

                //获取DataTable信息（源端table）
                int tableId = projectTopoTable.getTableId();
                DataTable dataTable = tableService.findTableById(tableId);

                payload = buildPaylodBase(projectTopoTableId,dataTable);

                //tableTopic 赋值
                tableTopic = offsetBean.getTopic();
                payloadOffset.put("topic",tableTopic);

                messageTopic = offsetBean.getTopoName()+"_"+"ctrl";
            }

            int partition = offsetBean.getPartition();
            String offset = offsetBean.getOffset();

            //构造payloadOffset的offset中的offsetParis信息
            if(StringUtils.isBlank(offset)){
                offsetParis.append(partition).append("->").append(",");
            /*}else if(StringUtils.equalsIgnoreCase(offset,"head")) {
                //begining
                offsetParis.append("begin").append(",");
            }else if(StringUtils.equalsIgnoreCase(offset,"latest")) {
                //end
                offsetParis.append("end").append(",");
            */}else {
                long offsetNum = Long.valueOf(offset);
                // offset
                offsetParis.append(partition).append("->").append(offsetNum).append(",");
            }
        }

        //如果offsetParis是空，说明没有partition更改，即需要发送的信息为空，所以不发消息，直接返回。
        if(offsetParis.length() < 1){
            logger.info("[start table] offsetParis is empty. projectTopoTableId: {} ",projectTopoTableId);
            return;
        }

        //StringBuffer 最后多加了一个','，移除
        payloadOffset.put("offsetParis",offsetParis.substring(0,offsetParis.length()-1));

        payloadOffsetArray.add(payloadOffset);
        payload.put("offset",payloadOffsetArray);

        //send
        toolSetService.sendProjectTableMessage(messageTopic,type,payload);

       /* 不更新table状态，只发送消息，让router去更新table状态
       //update table status
        ProjectTopoTable update = new ProjectTopoTable();
        update.setId(projectTopoTableId);
        update.setStatus(TableStatus.RUNNING.getValue());

        ResponseEntity<ResultEntity> result = sender.post(ServiceNames.KEEPER_SERVICE, "/projectTable/update",update);
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
            return result.getBody();
        return result.getBody();
        */
    }

    /**
     *
     * @param projectTopoTableId
     * @return
     *
     * 消息格式：
     * {
        "from": "dbus-web",
        "id": 1528856929837,
        "payload": {
            "projectTopoTableId": 70,
            "tableId": 2073,
            "dsName": "testdb",
            "schemaName": "test",
            "tableName": "user"
        },
        "type": "ROUTER_TOPOLOGY_TABLE_STOP",
        "timestamp": "2018-06-19 15:05:00.000"
        }
     */
    public void stop(Integer projectTopoTableId,String topoName){
        ProjectTopoTable projectTopoTable =getTableById(projectTopoTableId);
        if(projectTopoTable == null){
            logger.error("[stop table] table not found. projectTopoTableId:{} ",projectTopoTableId);
            throw new IllegalArgumentException("Table not found");
        }
        int tableId = projectTopoTable.getTableId();
        DataTable dataTable =tableService.findTableById(tableId);

        //payload基本信息
        JSONObject payload = buildPaylodBase(projectTopoTableId,dataTable);

        //stop的type
        String type = "ROUTER_TOPOLOGY_TABLE_STOP";
        //message的topic = topologyName + "_"+"ctrl"
        String topic = topoName +"_"+"ctrl";

        //send message
        toolSetService.sendProjectTableMessage(topic,type,payload);

        /* 不更新table状态，只发送消息，让router去更新table状态
        //update table status
        ProjectTopoTable update = new ProjectTopoTable();
        update.setId(projectTopoTableId);
        update.setStatus(TableStatus.STOPPED.getValue());

        ResponseEntity<ResultEntity> result = sender.post(ServiceNames.KEEPER_SERVICE, "/projectTable/update",update);
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
            return result.getBody();
        return result.getBody();
        */

    }

    /**
     * 根据projectTopoTable信息构造payload的基本信息
     * @return
     */
    private JSONObject buildPaylodBase(Integer projectTopoTableId,DataTable dataTable){
        //构造payload信息
        JSONObject payload = new JSONObject();
        payload.put("projectTopoTableId",projectTopoTableId);
        payload.put("tableId",dataTable.getId());
        payload.put("dsName",dataTable.getDsName());
        payload.put("schemaName",dataTable.getSchemaName());
        payload.put("tableName",dataTable.getTableName());

        return payload;
    }

    /**
     *
     * @param projectTopoTableId
     * @return
     *
     * message:
     * {
        "from": "dbus-web",
        "id": 1528856929837,
        "payload": {
        "projectTopoTableId": 70,
        "tableId": 2073,
        "dsName": "testdb",
        "schemaName": "test",
        "tableName": "user"
        },
        "type": "ROUTER_TOPOLOGY_TABLE_EFFECT",
        "timestamp": "2018-06-19 15:05:00.000"
        }
     */
    public void reload(Integer projectTopoTableId,String topoName){
        ProjectTopoTable projectTopoTable =getTableById(projectTopoTableId);
        if(projectTopoTable == null){
            logger.error("[reload table] table not found. projectTopoTableId:{} ",projectTopoTableId);
            throw new IllegalArgumentException("Table not found");
        }
        int tableId = projectTopoTable.getTableId();
        DataTable dataTable =tableService.findTableById(tableId);

        //payload基本信息
        JSONObject payload = buildPaylodBase(projectTopoTableId,dataTable);

        //reload的type
        String type = "ROUTER_TOPOLOGY_TABLE_EFFECT";

        //message的topic = projectTopoTable.inputTopic = dataTable.outputToic
        String topic = topoName+"_"+"ctrl";

        toolSetService.sendProjectTableMessage(topic,type,payload);
    }

    public ResultEntity deleteById(int tableId){
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectTable/delete-by-table-id/{id}",tableId);
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
            return result.getBody();
        //删除column信息
        result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectTable/delete-column-by-table-id/{id}",tableId);
        return result.getBody();
    }

    public ProjectTopoTable getTableById(int tableId){
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectTable/table/{tableId}",tableId);
        return result.getBody().getPayload(ProjectTopoTable.class);
    }

    /**
     * @return null: success; or: fail
     */
    //@Async
    public ResultEntity fullPull(Integer projectTableId, String resultTopic){

        //根据Id获取projectTableId 信息
        ProjectTopoTable projectTopoTable = getTableById(projectTableId);
        if(projectTopoTable == null){
            ResultEntity resultEntity = new ResultEntity();
            resultEntity.setStatus(MessageCode.TABLE_NOT_FOUND_BY_ID);
            resultEntity.setMessage(null); //根据message为空，需要读取i18n中的message信息
            logger.error("[send full-pull message error] Table not found, projectTableId:{}",projectTableId);
            return resultEntity;
        }
        int tableId = projectTopoTable.getTableId();
        int projectId = projectTopoTable.getProjectId();

        //判断Resource是否可以拉全量，如果不能，应该屏蔽
        ResponseEntity<ResultEntity> result = sender.get(ServiceNames.KEEPER_SERVICE, "/projectResource/{0}/{1}", projectId,tableId);
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
            return result.getBody();
        ProjectResource resource =result.getBody().getPayload(new TypeReference<ProjectResource>() {});
        if(resource.getFullpullEnableFlag() == ProjectResource.FULL_PULL_ENABLE_FALSE){
            logger.info("[project table full pull] resource is fullPullEnableFalse." +
                    " projectResourceId:{}，projectTableId:{}",resource.getId(),projectTableId);
            ResultEntity resultEntity = new ResultEntity();
            resultEntity.setStatus(MessageCode.TABLE_RESOURCE_FULL_PULL_FALSE);
            resultEntity.setMessage(null); //根据message为空，需要读取i18n中的message信息
            return resultEntity;
        }

        //根据projectTableId的tableId获取table信息
        DataTable dataTable =tableService.findTableById(tableId);
        //判断表类型是否支持拉全量操作
        DbusDatasourceType dsType = DbusDatasourceType.parse(dataTable.getDsType());
        if (DbusDatasourceType.ORACLE != dsType && DbusDatasourceType.MYSQL != dsType) {
            logger.error("Illegal datasource type:" + dataTable.getDsType());
			ResultEntity resultEntity = new ResultEntity();
			resultEntity.setStatus(MessageCode.TYPE_OF_TABLE_CAN_NOT_FULLPULL);
            return resultEntity;
        }

        result = sender.get(ServiceNames.KEEPER_SERVICE, "/projects/select/{id}", projectId);
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
            return result.getBody();
        Project projectMsg = result.getBody().getPayload(Project.class);

        //安全模式，如果需要新建topic，要插入acl
        try {
        }catch (Exception e){
            logger.error("[full pull ]add topic acl error.{}",e);
            ResultEntity resultEntity = new ResultEntity();
            resultEntity.setStatus(MessageCode.EXCEPTION);
            return resultEntity;
        }



        //构造controlMessage
        Date date = new Date();
        JSONObject message =fullPullService.buildMessage(date);
        JSONObject payload =fullPullService.buildPayload(resultTopic,date.getTime(),dataTable);
        JSONObject project = fullPullService.buildProject(projectTopoTable,projectMsg.getProjectName());
        message.put("payload",payload);
        message.put("project",project);

        //生成fullPullHistory对象
        FullPullHistory fullPullHistory = new FullPullHistory();
        fullPullHistory.setId(date.getTime());
        fullPullHistory.setType("indepent");
        fullPullHistory.setDsName(dataTable.getDsName());
        fullPullHistory.setSchemaName(dataTable.getSchemaName());
        fullPullHistory.setTableName(dataTable.getTableName());
        fullPullHistory.setState("init");
        fullPullHistory.setInitTime(new Date(fullPullHistory.getId()));
        fullPullHistory.setUpdateTime(fullPullHistory.getInitTime());
        fullPullHistory.setProjectName(projectMsg.getProjectName());
        fullPullHistory.setTopologyTableId(projectTableId);
        fullPullHistory.setTargetSinkId(projectTopoTable.getSinkId());
        fullPullHistory.setTargetSinkTopic(resultTopic);

        //发送消息
        String sendResult = fullPullService.sendMessage(dataTable,message.toJSONString(),fullPullHistory);
        if(StringUtils.equalsIgnoreCase(KeeperConstants.OK,sendResult)){
            logger.info("[send full-pull message] success! ");
            return null;
        }else {
            ResultEntity resultEntity = new ResultEntity();
            resultEntity.setStatus(MessageCode.INITIAL_LOAD_ERROR);
            resultEntity.setMessage(sendResult);
            logger.error("[send full-pull message] error! result: {}",sendResult);
            return resultEntity;
        }

    }

    /**
     * 所有的脱敏选择，包括默认的脱敏规则，以及项目下的plugins信息
     */
    public ResultEntity getAllEncoders(int projectId){

        //获得该项目下的encoderPlugins
        ResponseEntity<ResultEntity> responseEntity= sender.get(ServiceNames.KEEPER_SERVICE,"/encode-plugins/project-plugins/{0}",projectId);
        if(!responseEntity.getStatusCode().is2xxSuccessful() || !responseEntity.getBody().success()){
            logger.error("[get all encoders] get project encode-plugins error! projectId:{} ",projectId);
            return responseEntity.getBody();
        }
        List<EncodePlugins> projectEncodePlugins = responseEntity.getBody()
                .getPayload(new TypeReference<List<EncodePlugins>>() {});



        //构造返回信息
        AllEncodersBean encodersResult = new AllEncodersBean();
        encodersResult.setPlugins(projectEncodePlugins);

        //不直接构造ResultEntity，利用成功返回的responseEntity构造
        responseEntity.getBody().setPayload(encodersResult);

        return responseEntity.getBody();
    }

}
