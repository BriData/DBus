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


package com.creditease.dbus.controller;

import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.bean.AddSchemaTablesBean;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.domain.model.DataSchema;
import com.creditease.dbus.domain.model.DataSource;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.service.DataSchemaService;
import com.creditease.dbus.service.DataSourceService;
import com.creditease.dbus.service.schema.MongoSchemaFetcher;
import com.creditease.dbus.service.schema.SchemaFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by xiancangao on 2018/04/16.
 */
@RestController
@RequestMapping("/dataschema")
public class DataSchemaController extends BaseController {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private DataSchemaService service;
    @Autowired
    private DataSourceService dsService;

    @GetMapping("/searchSchemaAndDs")
    public ResultEntity search(@RequestParam(defaultValue = "1") Integer pageNum,
                               @RequestParam(defaultValue = "10") Integer pageSize,
                               Integer dsId, String schemaName) {
        return resultEntityBuilder().payload(service.searchSchemaAndDs(pageNum, pageSize, dsId, schemaName)).build();
    }

    @GetMapping("/searchSchema")
    public ResultEntity searchSchema(@RequestParam(required = false) Integer schemaId, @RequestParam(required = false) Integer dsId,
                                     @RequestParam(required = false) String schemaName) {
        return resultEntityBuilder().payload(service.searchSchema(schemaId, dsId, schemaName)).build();
    }

    @PostMapping("/insert")
    public ResultEntity addOne(@RequestBody DataSchema newOne) {
        return resultEntityBuilder().payload(service.insertOne(newOne)).build();
    }

    @GetMapping("/delete/{id}")
    public ResultEntity deleteById(@PathVariable Integer id) {
        return resultEntityBuilder().payload(service.deleteBySchemaId(id)).build();
    }

    @PostMapping("/update")
    public ResultEntity updateById(@RequestBody DataSchema updateOne) {
        return resultEntityBuilder().payload(service.update(updateOne)).build();
    }

    @PostMapping("/modifyDataSchemaStatus")
    public ResultEntity modifyDataSchemaStatus(@RequestBody Map<String, Object> param) {
        try {
            service.modifyDataSchemaStatus(param);
            return resultEntityBuilder().payload("OK").build();
        } catch (Exception ex) {
            logger.error("Error encountered while active DateSource with parameter:{}", param, ex);
            return resultEntityBuilder().status(MessageCode.DATASCHEMA_CHANGE_STATUS_FAILED).build();
        }
    }

    /**
     * 源端数据访问
     */
    @GetMapping("/fetchSchemaFromSource")
    public ResultEntity fetchSchemaFromSource(@RequestParam String dsName) {
        try {
            DataSource ds = null;
            List<DataSchema> list = new ArrayList<>();
            List<DataSource> dsList = dsService.getDataSourceByName(dsName);

            if (!dsList.isEmpty()) {
                ds = dsList.get(0);
                if (DbusDatasourceType.stringEqual(ds.getDsType(), DbusDatasourceType.MYSQL)
                        || DbusDatasourceType.stringEqual(ds.getDsType(), DbusDatasourceType.ORACLE)) {
                    SchemaFetcher fetcher = SchemaFetcher.getFetcher(ds);
                    list = fetcher.fetchSchema();
                } else if (DbusDatasourceType.stringEqual(ds.getDsType(), DbusDatasourceType.MONGO)) {
                    MongoSchemaFetcher fetcher = new MongoSchemaFetcher(ds);
                    list = fetcher.fetchSchema();
                } else {
                    throw new IllegalArgumentException("Unsupported datasource type");
                }
                if (!list.isEmpty()) {
                    for (int i = 0; i < list.size(); i++) {
                        list.get(i).setDsId(ds.getId());
                        list.get(i).setStatus(ds.getStatus());
                        list.get(i).setSrcTopic(ds.getDsName() + "." + list.get(i).getSchemaName());
                        list.get(i).setTargetTopic(ds.getDsName() + "." + list.get(i).getSchemaName() + ".result");
                    }
                }
            }
            return resultEntityBuilder().payload(list).build();
        } catch (Exception e) {
            logger.error("Error encountered while fetch schema from source with parameter:{}", dsName, e);
            return resultEntityBuilder().status(MessageCode.DATASCHEMA_FETCH_FROM_SOURCE_FAILED).build();
        }
    }

    @GetMapping("/get/{id}")
    public ResultEntity getById(@PathVariable Integer id) {
        return resultEntityBuilder().payload(service.selectById(id)).build();
    }

    /**
     * 添加schema和table
     *
     * @return
     */
    @PostMapping("/schema-and-tables")
    public ResultEntity insertSchemaAndTables(@RequestBody List<AddSchemaTablesBean> schemaAndTablesList) {
        try {
            int i = service.addSchemaAndTables(schemaAndTablesList);
            return resultEntityBuilder().payload(i).build();
        } catch (Exception e) {
            return resultEntityBuilder().status(MessageCode.DATASCHEMA_PARAM_FOARMAT_ERROR).build();
        }
    }

    /**
     * 获取源端schema信息
     */
    @GetMapping("/source-schemas")
    public ResultEntity getSourceSchemas(@RequestParam Integer dsId) {
        try {
            return resultEntityBuilder().payload(service.fetchSchemas(dsId)).build();
        } catch (Exception e) {
            logger.error("[source schemas] Exception:{}", e);
            return resultEntityBuilder().status(MessageCode.DATASCHEMA_DS_TYPE_ERROR).build();
        }
    }

    /**
     * 根据dsId和schemaName
     */
    @GetMapping("/manager-schema")
    public ResultEntity getManagerSchema(@RequestParam int dsId, @RequestParam String schemaName) {
        return resultEntityBuilder().payload(service.findSchema(dsId, schemaName)).build();
    }

    @PostMapping("/moveSourceSchema")
    public ResultEntity moveSourceSchema(@RequestBody Map<String, Object> param) {
        try {
            return resultEntityBuilder().status(service.moveSourceSchema(param)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while request moveSourceSchema.", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }
}
