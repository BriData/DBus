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

import com.alibaba.fastjson.JSON;
import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.bean.DataTableBean;
import com.creditease.dbus.bean.DataTableMetaBean;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.domain.model.TableMeta;
import com.creditease.dbus.service.DataHubService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/datahub")
public class DataHubController extends BaseController {

    @Autowired
    private DataHubService dataHubService;

    /**
     * 根据schemaId和tableNames查询表详情
     *
     * @param param
     * @return
     */
    @PostMapping("/getDBusTableInfo")
    public ResultEntity getDBusTableInfo(@RequestBody HashMap<String, Object> param) {
        try {
            logger.info("收到获取表详情信息请求,param:{}", param);
            return dataHubService.getDBusTableInfo(param);
        } catch (Exception e) {
            logger.error("Exception encountered while getBasicConf ", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 根据dsName和schemaName查询dbus接入的表详情
     *
     * @param dsName
     * @param dbName
     * @return
     */
    @GetMapping("/getDBusSourceInfo")
    public ResultEntity getDBusSourceInfo(@RequestParam String dsName, @RequestParam String dbName) {
        try {
            logger.info("收到获取指定数据源详情接口.dsName:{},dbName:{}", dsName, dbName);
            List<DataTableBean> dataTables = dataHubService.getDBusSourceInfo(dsName, dbName);
            return resultEntityBuilder().payload(JSON.toJSONString(dataTables)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while getAllDBusSourceInfo ", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping("/getDBusTableStatus")
    public ResultEntity getDBusTableStatus(@RequestBody Map<Long, List<String>> param) {
        try {
            List<DataTableBean> dataTableList = dataHubService.getDBusTableStatusBySchemaIdAndTableNames(param);
            return resultEntityBuilder().payload(JSON.toJSONString(dataTableList)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while getAllDBusSourceInfo ", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping("/getDBusTableFields/{tableId}")
    public ResultEntity getDBusTableFields(@PathVariable Integer tableId) {
        try {
            List<TableMeta> tableMetaList = dataHubService.getDBusTableFields(tableId);
            return resultEntityBuilder().payload(JSON.toJSONString(tableMetaList)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while getAllDBusSourceInfo ", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * @param startTime yyyy-MM-dd HH:mm:ss 格式的字符串时间
     * @param endTime   yyyy-MM-dd HH:mm:ss 格式的字符串时间
     * @return
     */
    @GetMapping("/getDdlDBusTableFields")
    public ResultEntity getDdlDBusTableFields(@RequestParam String startTime, @RequestParam String endTime, Integer dbusId, String tableName) {
        try {
            List<DataTableMetaBean> dataTableMetaBeans = dataHubService.getDdlDBusTableFields(startTime, endTime, dbusId, tableName);
            return resultEntityBuilder().payload(JSON.toJSONString(dataTableMetaBeans)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while getDdlDBusTableFields ", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }


    @GetMapping("/getDbConnectInfoByDbId/{dbId}")
    public ResultEntity getDbConnectInfoByDbId(@PathVariable Long dbId) {
        try {
            return resultEntityBuilder().payload(JSON.toJSONString(dataHubService.getDbConnectInfoByDbId(dbId))).build();
        } catch (Exception e) {
            logger.error("Exception encountered while getDdlDBusTableFields ", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping("/getAllDataSourceInfo")
    public ResultEntity getAllDataSourceInfo(String dsName) {
        try {
            logger.info("收到获取数据源详情接口.dsName:{}", dsName);
            return resultEntityBuilder().payload(dataHubService.getAllDataSourceInfo(dsName)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while getAllDataSourceInfo ", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping("/getTableRows")
    public ResultEntity getTableRows(@RequestParam Long schemaId, @RequestParam String tableName) {
        try {
            logger.info("收到获取表数据量请求.schemaId:{},tableName:{}", schemaId, tableName);
            return resultEntityBuilder().payload(dataHubService.getTableRows(schemaId, tableName)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while getAllDataSourceInfo ", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

}
