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
import com.creditease.dbus.bean.AddSchemaAndTablesBean;
import com.creditease.dbus.bean.SchemaAndTablesInfoBean;
import com.creditease.dbus.bean.SourceTablesBean;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.constant.ServiceNames;
import com.creditease.dbus.domain.model.DataSchema;
import com.creditease.dbus.domain.model.DataSource;
import com.creditease.dbus.domain.model.DataTable;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.net.URLDecoder;
import java.util.*;

/**
 * User: 尹宏春
 * Date: 2018-05-08
 * Time: 上午11:38
 */
@Service
public class DataSchemaService {

    @Autowired
    private RequestSender sender;

    @Autowired
    private ToolSetService toolSetService;

    private static final String KEEPER_SERVICE = ServiceNames.KEEPER_SERVICE;
    private Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * datasource首页的搜索
     *
     * @param queryString param:dsName,if ds=null get all
     */
    public ResultEntity searchSchemaAndDs(String queryString) throws Exception {
        if (StringUtils.isNotBlank(queryString)) {
            queryString = URLDecoder.decode(queryString, "UTF-8");
        }
        ResponseEntity<ResultEntity> result = sender.get(KEEPER_SERVICE, "/dataschema/searchSchemaAndDs", queryString);
        return result.getBody();
    }

    public ResultEntity searchSchema(String queryString) {
        ResponseEntity<ResultEntity> result = sender.get(KEEPER_SERVICE, "/dataschema/searchSchema", queryString);
        return result.getBody();
    }

    public ResultEntity insertOne(DataSchema dataSchema) {
        ResponseEntity<ResultEntity> result = sender.post(KEEPER_SERVICE, "/dataschema/insert", dataSchema);
        return result.getBody();
    }

    public ResultEntity update(DataSchema dataSchema) {
        ResponseEntity<ResultEntity> result = sender.post(KEEPER_SERVICE, "/dataschema/update", dataSchema);
        return result.getBody();
    }

    public ResultEntity delete(Integer id) {
        ResponseEntity<ResultEntity> result = sender.get(KEEPER_SERVICE, "/dataschema/delete/{id}", id);
        return result.getBody();
    }

    public ResultEntity modifyDataSchemaStatus(Long id, String status) {
        Map<String, Object> param = new HashMap<>();
        param.put("id", id);
        param.put("status", status);
        ResponseEntity<ResultEntity> result = sender.post(KEEPER_SERVICE, "/dataschema/modifyDataSchemaStatus", param);
         return result.getBody();
    }

    public ResultEntity fetchSchemaFromSource(String queryString) {
        ResponseEntity<ResultEntity> result = sender.get(KEEPER_SERVICE, "/dataschema/fetchSchemaFromSource", queryString);
        return result.getBody();
    }

    public ResultEntity fetchSchemaFromSourceByDsId(String queryString) {
        ResponseEntity<ResultEntity> result = sender.get(KEEPER_SERVICE, "/dataschema/source-schemas", queryString);
        return result.getBody();
    }

    public ResultEntity getSchemaAndTablesInfo(int dsId, String dsName,String schemaName){
        ResponseEntity<ResultEntity> result = sender.get(KEEPER_SERVICE, "/dataschema/manager-schema",
                "dsId="+dsId+"&schemaName="+schemaName);
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
            return result.getBody();
        //获得schema信息，对应以前的checkManagerSchema
        DataSchema schemaInfo =result.getBody().getPayload(new TypeReference<DataSchema>() {});

        result = sender.get(ServiceNames.KEEPER_SERVICE, "/tables/tables-to-add",
                "dsId="+dsId+"&schemaName="+schemaName+"&dsName="+dsName);
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
            return result.getBody();
        //获得tables信息
        List<Map<String, Object>> tables = result.getBody().getPayload(new TypeReference<List<Map<String, Object>>>() {});

        //拼接
        Map<String,Object> resultInfo = new JSONObject();
        resultInfo.put("schema",schemaInfo);
        resultInfo.put("tables", tables);

        result.getBody().setPayload(resultInfo);
        return result.getBody();
    }

    public ResultEntity addSchemaAndTablesInfo(AddSchemaAndTablesBean addInfos){
        DataSource dataSource = addInfos.getDataSource();
        //先对dsType校验，如果是unknown的直接返回
        String dsType = dataSource.getDsType();
        DbusDatasourceType datasourceType =DbusDatasourceType.parse(dsType);
        if(datasourceType == DbusDatasourceType.UNKNOWN){
            logger.info("[add schema and table] datasource type is unknown. AddSchemaAndTablesBean:{}",addInfos);
            ResultEntity resultEntity = new ResultEntity();
            resultEntity.setStatus(MessageCode.DATASOURCE_TYPE_UNKNOWN);
            return resultEntity;
        }

        List<SchemaAndTablesInfoBean> infos = addInfos.getSchemaAndTables();
        /* 插入schema和tables到管理库 */
        ResponseEntity<ResultEntity> result = sender.post(KEEPER_SERVICE, "/dataschema/schema-and-tables",infos);
        if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
            return result.getBody();

        /* 插入table的信息到源端库: LOG_XX 类型不需要*/
        if(DbusDatasourceType.ORACLE == datasourceType || DbusDatasourceType.MYSQL == datasourceType){
            //构造sourceTablesBean的参数
            SourceTablesBean sourceTablesBean = new SourceTablesBean();
            List<DataTable> sourceTables = new ArrayList<>();
            //将要添加的table都加入到sourceTablesBean的list中
            for(SchemaAndTablesInfoBean schemaAndTables: infos){
                sourceTables.addAll(schemaAndTables.getTables());
            }
            //构造sourceTablesBean
            sourceTablesBean.setDsId(dataSource.getId());
            sourceTablesBean.setSourceTables(sourceTables);

            //插入table
            result = sender.post(KEEPER_SERVICE, "/tables/source-tables",sourceTablesBean);
            if (!result.getStatusCode().is2xxSuccessful() || !result.getBody().success())
                return result.getBody();
        }

        /* 添加成功，发送control message */
        toolSetService.sendCtrlMessageEasy(dataSource.getId(),dataSource.getDsName(),dataSource.getDsType());

        return new ResultEntity();
    }

	public int countBySchemaId(Integer id) {
        return sender.get(ServiceNames.KEEPER_SERVICE, "/projectTable/count-by-schema-id/{0}", id)
                .getBody().getPayload(Integer.class);
	}
}
