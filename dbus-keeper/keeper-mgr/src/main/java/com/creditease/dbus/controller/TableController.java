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

package com.creditease.dbus.controller;

import com.creditease.dbus.annotation.AdminPrivilege;
import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.bean.ExecuteSqlBean;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.domain.model.DataTable;
import com.creditease.dbus.service.TableService;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by xiancangao on 2018/05/16.
 */
@RestController
@RequestMapping("/tables")
@AdminPrivilege
public class TableController extends BaseController {

    @Autowired
    private TableService tableService;


    /**
     * 激活指定的table,使该table生效
     *
     * @param id table的ID
     */
    @PostMapping(path = "/activate/{id:\\d+}", consumes = "application/json")
    public ResultEntity activateDataTable(@PathVariable("id") Integer id, @RequestBody Map<String, String> map) {
        try {
            Integer result = tableService.activateDataTable(id, map);
            if (result != null) {
                return resultEntityBuilder().status(result).build();
            }
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while request activateDataTable with parameter:(id:{},map:{})", id, map, e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 使table无效
     *
     * @param id table的ID
     */
    @GetMapping("/deactivate/{id:\\d+}")
    public ResultEntity deactivateDataTable(@PathVariable("id") Integer id) {
        try {
            Integer result = tableService.deactivateDataTable(id);
            if (result != null) {
                return resultEntityBuilder().status(result).build();
            }
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while request deactivateDataTable Table with parameter:(id:{})", id, e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 查询对应tableId的表字段脱敏列
     *
     * @param tableId
     * @return
     */
    @GetMapping("/desensitization/{tableId}")
    public ResultEntity desensitization(@PathVariable Integer tableId) {
        try {
            return tableService.desensitization(tableId);
        } catch (Exception e) {
            logger.error("Exception encountered while request desensitization with parameter:(tableId:{})", tableId, e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 根据id获取对应表的列信息
     *
     * @param tableId
     * @return
     */
    @GetMapping("/fetchTableColumns/{tableId}")
    public ResultEntity fetchTableColumns(@PathVariable Integer tableId) {
        try {
            return tableService.fetchTableColumns(tableId);
        } catch (Exception e) {
            logger.error("Exception encountered while request fetchTableColumns with parameter:(tableId:{})", tableId, e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 根据id更新table
     *
     * @param
     * @param dataTable 数据
     * @return 更新状态
     */
    @PostMapping(path = "/updateTable", consumes = "application/json")
    public ResultEntity updateTable(@RequestBody DataTable dataTable) {
        try {
            return tableService.updateTable(dataTable);
        } catch (Exception e) {
            logger.error("Exception encountered while request updateTable .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping("/delete/{tableId}")
    public ResultEntity deleteTable(@PathVariable Integer tableId) {
        try {
            int size = tableService.countActiveTables(tableId);
            if (size != 0) {
                return resultEntityBuilder().status(MessageCode.TABLE_ALREADY_BE_USING).build();
            }
            ResultEntity resultEntity = tableService.deleteTable(tableId);
            if (resultEntity.getMessage() != null) {
                return resultEntity;
            }
            return resultEntityBuilder().status(resultEntity.getStatus()).build();
        } catch (Exception e) {
            logger.error("Exception encountered while request deleteTable .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping("/confirmStatusChange/{tableId}")
    public ResultEntity confirmStatusChange(@PathVariable Integer tableId) {
        try {
            return tableService.confirmStatusChange(tableId);
        } catch (Exception e) {
            logger.error("Exception encountered while request confirmStatusChange Table .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping("/getVersionListByTableId/{tableId}")
    public ResultEntity getVersionListByTableId(@PathVariable Integer tableId) {
        try {
            return tableService.getVersionListByTableId(tableId);
        } catch (Exception e) {
            logger.error("Exception encountered while request getVersionListByTableId Table .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping("/getVersionDetail/{versionId1}/{versionId2}")
    public ResultEntity getVersionDetail(@PathVariable Integer versionId1, @PathVariable Integer versionId2) {
        try {
            return tableService.getVersionDetail(versionId1, versionId2);
        } catch (Exception e) {
            logger.error("Exception encountered while request getVersionDetail Table .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * @param : Long dsId,String schemaName,String tableName
     *          根据DSList获取到的dsId,或输入的schemaName和tableName查询
     */
    @GetMapping("/find")
    public ResultEntity findTables(HttpServletRequest request) throws Exception {
        String role = currentUserRole();
        if (!StringUtils.equalsIgnoreCase(role, "admin")) {
            return resultEntityBuilder().status(MessageCode.AUTHORIZATION_FAILURE).build();
        }
        return tableService.findTables(URLDecoder.decode(request.getQueryString(), "UTF-8"));
    }

    @GetMapping("/findAll")
    public ResultEntity findAllTables(HttpServletRequest request) throws Exception {
        String role = currentUserRole();
        if (!StringUtils.equalsIgnoreCase(role, "admin")) {
            return resultEntityBuilder().status(MessageCode.AUTHORIZATION_FAILURE).build();
        }
        return tableService.findAllTables(URLDecoder.decode(request.getQueryString(), "UTF-8"));
    }

    @GetMapping("/fetchEncodeAlgorithms")
    public ResultEntity fetchEncodeAlgorithms() {
        return tableService.fetchEncodeAlgorithms();
    }

    @PostMapping(path = "/changeDesensitization", consumes = "application/json")
    public ResultEntity changeDesensitization(@RequestBody Map<String, Map<String, Object>> param) {
        try {
            return tableService.changeDesensitization(param);
        } catch (Exception e) {
            logger.error("Exception encountered while request changeDesensitization Table .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping("/getAllRuleGroup/{tableId}")
    public ResultEntity getAllRuleGroup(@PathVariable Integer tableId) {
        try {
            return tableService.getAllRuleGroup(tableId);
        } catch (Exception e) {
            logger.error("Exception encountered while request getAllRuleGroup Table .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }


    @GetMapping("/updateRuleGroup")
    public ResultEntity updateRuleGroup(HttpServletRequest request) {
        try {
            return tableService.updateRuleGroup(URLDecoder.decode(request.getQueryString(), "UTF-8"));
        } catch (Exception e) {
            logger.error("Exception encountered while request updateRuleGroup Table .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }


    @GetMapping("/deleteRuleGroup/{groupId}")
    public ResultEntity deleteRuleGroup(@PathVariable Integer groupId) {
        try {
            return tableService.deleteRuleGroup(groupId);
        } catch (Exception e) {
            logger.error("Exception encountered while request getAllRuleGroup Table .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping("/addGroup")
    public ResultEntity addGroup(HttpServletRequest request) {
        try {
            return tableService.addGroup(URLDecoder.decode(request.getQueryString(), "UTF-8"));
        } catch (Exception e) {
            logger.error("Exception encountered while request addGroup Table .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping("/cloneRuleGroup")
    public ResultEntity cloneRuleGroup(HttpServletRequest request) {
        try {
            return tableService.cloneRuleGroup(URLDecoder.decode(request.getQueryString(), "UTF-8"));
        } catch (Exception e) {
            logger.error("Exception encountered while request cloneRuleGroup Table .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping("/diffGroupRule/{tableId}")
    public ResultEntity diffGroupRule(@PathVariable Integer tableId) {
        try {
            return tableService.diffGroupRule(tableId);
        } catch (Exception e) {
            logger.error("Exception encountered while request diffGroupRule Table .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping("/upgradeVersion")
    public ResultEntity upgradeVersion(HttpServletRequest request) {
        try {
            return tableService.upgradeVersion(request.getQueryString());
        } catch (Exception e) {
            logger.error("Exception encountered while request upgradeVersion Table .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping("/getAllRules")
    public ResultEntity getAllRules(@RequestParam Map<String, Object> map) {
        try {
            Map<String, Object> result = tableService.getAllRules(map);
            return resultEntityBuilder().payload(result).build();
        } catch (Exception e) {
            logger.error("Exception encountered while request getAllRules Table .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping(path = "/saveAllRules", consumes = "application/json")
    public ResultEntity saveAllRules(@RequestBody Map<String, Object> map) {
        try {
            return tableService.saveAllRules(map);
        } catch (Exception e) {
            logger.error("Exception encountered while request saveAllRules Table .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping(path = "/executeRules", consumes = "application/json")
    public ResultEntity executeRules(@RequestBody Map<String, Object> map) {
        try {
            return tableService.executeRules(map);
        } catch (Exception e) {
            logger.error("Exception encountered while request executeRules Table .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * data-table中datasource的下拉列表
     *
     * @return dsId + dsType + dsName
     */
    @GetMapping("/DSList")
    public ResultEntity getDSList() {
        return tableService.getDSList();
    }

    @GetMapping("/{tableId}")
    public ResultEntity getById(@PathVariable Integer tableId) {
        return tableService.findById(tableId);
    }

    @PostMapping("/executeSql")
    public ResultEntity executeSql(@RequestBody ExecuteSqlBean executeSqlBean) {
        try {
            return tableService.executeSql(executeSqlBean);
        } catch (Exception e) {
            logger.error("Exception encountered while request executeSql Table .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @GetMapping("/tables-to-add")
    public ResultEntity getTablesToAdd(HttpServletRequest request) throws Exception {
        return tableService.findTablesToAdd(URLDecoder.decode(request.getQueryString(), "UTF-8"));
    }

    @GetMapping("/riderSearch")
    public ResultEntity riderSearch() throws Exception {
        try {
            Integer userId = currentUserId();
            String userRole = currentUserRole();
            return resultEntityBuilder().payload(tableService.riderSearch(userId, userRole)).build();
        } catch (Exception e) {
            logger.error("Exception encountered while request riderSearch .", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * table 级别拖回重跑
     *
     * @param dsId
     * @param dsName
     * @param schemaName
     * @param offset
     * @return
     */
    @GetMapping("/rerun")
    public ResultEntity rerun(Integer dsId, String dsName, String schemaName, String tableName, Long offset) {
        try {
            int result = tableService.rerun(dsId, dsName, schemaName, tableName, offset);
            return resultEntityBuilder().status(result).build();
        } catch (Exception e) {
            logger.error("Exception encountered while rerun table ({})", dsName + "." + schemaName + "." + tableName, e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 批量激活止table
     *
     * @param
     */
    @PostMapping(path = "/batchStartTableByTableIds")
    public ResultEntity batchStartTableByTableIds(@RequestBody ArrayList<Integer> tableIds) {
        try {
            ResultEntity resultEntity = tableService.batchStartTableByTableIds(tableIds);
            if (StringUtils.isBlank(resultEntity.getMessage())) {
                return resultEntityBuilder().status(resultEntity.getStatus()).build();
            }
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while request batchStartTableByTableIds.", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    /**
     * 批量停止table
     *
     * @param
     */
    @PostMapping(path = "/batchStopTableByTableIds")
    public ResultEntity batchStopTableByTableIds(@RequestBody ArrayList<Integer> tableIds) {
        try {
            ResultEntity resultEntity = tableService.batchStopTableByTableIds(tableIds);
            if (StringUtils.isBlank(resultEntity.getMessage())) {
                return resultEntityBuilder().status(resultEntity.getStatus()).build();
            }
            return resultEntityBuilder().build();
        } catch (Exception e) {
            logger.error("Exception encountered while request startOrStopTableByTableIds.", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

    @PostMapping("/importRulesByTableId/{tableId}")
    public ResultEntity importRulesByTableId(@PathVariable Integer tableId,
                                             @RequestParam MultipartFile uploadFile) {
        try {
            return tableService.importRulesByTableId(tableId, uploadFile);
        } catch (Exception e) {
            logger.error("Exception encountered while request importRulesByTableId.", e);
            return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
        }
    }

	@GetMapping("/exportRulesByTableId/{tableId}")
	public void exportRulesByTableId(@PathVariable Integer tableId, HttpServletResponse response) {
		try {
			tableService.exportRulesByTableId(tableId, response);
		} catch (Exception e) {
			logger.error("Exception encountered while request exportRulesByTableId.", e);
		}
	}

}
