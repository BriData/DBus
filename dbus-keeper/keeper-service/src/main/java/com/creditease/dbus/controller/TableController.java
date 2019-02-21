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

import com.creditease.dbus.base.BaseController;
import com.creditease.dbus.base.ResultEntity;
import com.creditease.dbus.bean.SourceTablesBean;
import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.domain.model.DataTable;
import com.creditease.dbus.domain.model.DataTableRule;
import com.creditease.dbus.domain.model.DesensitizationInformation;
import com.creditease.dbus.domain.model.RuleInfo;
import com.creditease.dbus.service.TableService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by xiancangao on 2018/05/16.
 */
@RestController
@RequestMapping("/tables")
public class TableController extends BaseController {
	@Autowired
	private TableService tableService;

	/**
	 * @param tebleId
	 * @return
	 */
	@GetMapping("/fetchTableColumns/{tebleId}")
	public ResultEntity fetchTableColumns(@PathVariable Integer tebleId) {
		try {
			return tableService.fetchTableColumns(tebleId);
		} catch (Exception e) {
			logger.error("Error encountered while fetchTableColumns Table .", e);
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
	@PostMapping(path = "/update", consumes = "application/json")
	public ResultEntity update(@RequestBody DataTable dataTable) {
		try {
			tableService.update(dataTable);
			return resultEntityBuilder().build();
		} catch (Exception e) {
			logger.error("Error encountered while update Table .", e);
			return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
		}
	}

	@GetMapping("/delete/{tableId}")
	public ResultEntity delete(@PathVariable Integer tableId) {
		try {
			int id = tableService.deleteTable(Integer.parseInt(tableId.toString()));
			return resultEntityBuilder().payload(id).build();
		} catch (Exception e) {
			logger.error("Error encountered while delete Table .", e);
			return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
		}
	}

	@GetMapping("/confirmStatusChange/{tableId}")
	public ResultEntity confirmStatusChange(@PathVariable Integer tableId) {
		try {
			int id = tableService.confirmStatusChange(tableId);
			if (id == 0) {
				return resultEntityBuilder().status(MessageCode.SEND_CONFIRMATION_MESSAGE_FAILED).build();
			}
			return resultEntityBuilder().payload(id).build();
		} catch (Exception e) {
			logger.error("Error encountered while confirmStatusChange Table .", e);
			return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
		}
	}

	@GetMapping("/get-version-detail/{versionId1}/{versionId2}")
	public ResultEntity getVersionDetail(@PathVariable Integer versionId1, @PathVariable Integer versionId2) {
		try {
			Map<String, List<Map<String, Object>>> result = tableService.getVersionDetail(versionId1, versionId2);
			return resultEntityBuilder().payload(result).build();
		} catch (Exception e) {
			logger.error("Error encountered while getVersionDetail Table .", e);
			return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
		}
	}

	@GetMapping("/getDesensitizationInfo/{tableId}")
	public ResultEntity getDesensitizationInfo(@PathVariable Integer tableId) {
		try {
			List<DesensitizationInformation> result = tableService.getDesensitizationInfo(tableId);
			return resultEntityBuilder().payload(result).build();
		} catch (Exception e) {
			logger.error("Error encountered while getDesensitizationInfo Table .", e);
			return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
		}
	}

	@PostMapping(path = "/change-desensitization", consumes = "application/json")
	public ResultEntity changeDesensitization(@RequestBody Map<String, Map<String, Object>> param) {
		try {
			tableService.changeDesensitization(param);
			return resultEntityBuilder().build();
		} catch (Exception e) {
			logger.error("Error encountered while changeDesensitization Table .", e);
			return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
		}
	}

	@GetMapping("/get-all-rule-group/{tableId}")
	public ResultEntity getAllRuleGroup(@PathVariable Integer tableId) {
		try {
			Map<String, Object> result = tableService.getAllRuleGroup(tableId);
			return resultEntityBuilder().payload(result).build();
		} catch (Exception e) {
			logger.error("Error encountered while getAllRuleGroup Table .", e);
			return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
		}
	}

	@GetMapping("/update-rule-group")
	public ResultEntity updateRuleGroup(@RequestParam Map<String, Object> map) {
		try {
			int result = tableService.updateRuleGroup(map);
			return resultEntityBuilder().payload(result).build();
		} catch (Exception e) {
			logger.error("Error encountered while updateRuleGroup Table .", e);
			return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
		}
	}

	@GetMapping("/delete-rule-group/{groupId}")
	public ResultEntity deleteRuleGroup(@PathVariable Integer groupId) {
		try {
			tableService.deleteRuleGroup(groupId);
			return resultEntityBuilder().build();
		} catch (Exception e) {
			logger.error("Error encountered while deleteRuleGroup Table .", e);
			return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
		}
	}

	@GetMapping("/add-group")
	public ResultEntity addGroup(@RequestParam Map<String, Object> map) {
		try {
			int result = tableService.addGroup(map);
			return resultEntityBuilder().payload(result).build();
		} catch (Exception e) {
			logger.error("Error encountered while addGroup Table .", e);
			return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
		}
	}

	@GetMapping("/clone-rule-group")
	public ResultEntity cloneRuleGroup(@RequestParam Map<String, Object> map) {
		try {
			Map<String, Object> result = tableService.cloneRuleGroup(map);
			return resultEntityBuilder().payload(result).build();
		} catch (Exception e) {
			logger.error("Error encountered while cloneRuleGroup Table .", e);
			return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
		}
	}

	@GetMapping("/diff-group-rule/{tableId}")
	public ResultEntity diffGroupRule(@PathVariable Integer tableId) {
		try {
			List<RuleInfo> result = tableService.diffGroupRule(tableId);
			return resultEntityBuilder().payload(result).build();
		} catch (Exception e) {
			logger.error("Error encountered while diffGroupRule Table .", e);
			return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
		}
	}

	@GetMapping("/up-grade-version")
	public ResultEntity upgradeVersion(@RequestParam Map<String, Object> map) {
		try {
			String result = tableService.upgradeVersion(map);
			if (StringUtils.isNotBlank(result)) {
				return resultEntityBuilder().status(MessageCode.USE_THE_DIFF_CONTRAST).build(result);
			}
			return resultEntityBuilder().build();
		} catch (Exception e) {
			logger.error("Error encountered while upgradeVersion Table .", e);
			return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
		}
	}

	@GetMapping("/get-all-rules/{groupId}")
	public ResultEntity getAllRules(@PathVariable Integer groupId) {
		try {
			List<DataTableRule> allRules = tableService.getAllRules(groupId);
			return resultEntityBuilder().payload(allRules).build();
		} catch (Exception e) {
			logger.error("Error encountered while getAllRules Table .", e);
			return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
		}
	}


	@PostMapping(path = "/save-all-rules", consumes = "application/json")
	public ResultEntity saveAllRules(@RequestBody Map<String, Object> map) {
		try {
			tableService.saveAllRules(map);
			return resultEntityBuilder().build();
		} catch (Exception e) {
			logger.error("Error encountered while saveAllRules Table .", e);
			return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
		}
	}

	@GetMapping("/find")
	public ResultEntity getTables(Integer dsId, String schemaName, String tableName,
	                              @RequestParam Integer pageNum,
	                              @RequestParam Integer pageSize) {
		return resultEntityBuilder().payload(tableService.getTablesInPages(dsId, schemaName, tableName, pageNum, pageSize)).build();
	}

	@GetMapping("/findAll")
	public ResultEntity getAllTables(Integer dsId, String schemaName) {
		return resultEntityBuilder().payload(tableService.getSchemaTables(dsId, schemaName)).build();
	}

	/**
	 * @return dsId + dsType + dsName
	 */
	@GetMapping("/DSList")
	public ResultEntity getDSList() {
		return resultEntityBuilder().payload(tableService.getDSList()).build();
	}

	@GetMapping("/get/{id}")
	public ResultEntity getTableById(@PathVariable Integer id) {
		DataTable dataTable = tableService.getTableById(id);
		return resultEntityBuilder().payload(dataTable).build();
	}

	@GetMapping("/get-by-schema-id/{id}")
	public ResultEntity getTablesBySchemaID(@PathVariable Integer id) {
		List<DataTable> dataTables = tableService.getTablesBySchemaID(id);
		return resultEntityBuilder().payload(dataTables).build();
	}

	@GetMapping("/{tableId}")
	public ResultEntity getById(@PathVariable Integer tableId) {
		return resultEntityBuilder().payload(tableService.getById(tableId)).build();
	}

	@PostMapping("/execute-sql")
	public ResultEntity executeSql(@RequestBody Map<String, Object> map) {
		try {
			return resultEntityBuilder().payload(tableService.executeSql(map)).build();
		} catch (Exception e) {
			logger.error("Error encountered while executeSql Table .", e);
			return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
		}
	}

	/**
	 * 获取某schema下已添加的table
	 */
	@GetMapping("schema-tables")
	public ResultEntity getSchemaTables(@RequestParam int dsId, String schemaName) {
		return resultEntityBuilder().payload(tableService.getSchemaTables(dsId, schemaName)).build();
	}

	/**
	 * 获取可添加的table
	 */
	@GetMapping("tables-to-add")
	public ResultEntity getTablesToAdd(@RequestParam int dsId, @RequestParam String dsName, @RequestParam String schemaName) {
		try {
			List<Map<String, Object>> result = tableService.tableList(dsId, dsName, schemaName);
			return resultEntityBuilder().payload(result).build();
		} catch (Exception e) {
			return resultEntityBuilder().status(MessageCode.FETCH_TABLE_ERROR).message(e.getMessage()).build();
		}

	}

	@GetMapping("get-by-ds-schema-table-name")
	public ResultEntity getByDsSchemaTableName(@RequestParam String dsName, @RequestParam String schemaName, @RequestParam String tableName) {
		return resultEntityBuilder().payload(tableService.getByDsSchemaTableName(dsName, schemaName, tableName)).build();
	}

	/**
	 * oracle新增table源端处理
	 * 1.校验是否打开全量补充日志
	 * 2.在源端dbus_tables表插入相关信息
	 *
	 * @return ResultEntity
	 */
	@PostMapping("/source-tables")
	public ResultEntity sourceTablesHandler(@RequestBody SourceTablesBean sourceTables) {
		try {
			ResultEntity resultEntity = tableService.sourceTablesHandler(sourceTables);
			//Service层构造的信息，无法读取i18n的内容，在这里重新构造下
			return resultEntityBuilder().status(resultEntity.getStatus()).build();
		} catch (Exception e) {
			logger.error("Error encountered while sourceTablesHandler Table .", e);
			return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
		}
	}

	@PostMapping("/manager-table")
	public ResultEntity insertManagerTables(@RequestBody DataTable table) {
		return resultEntityBuilder().payload(tableService.insertManageTable(table)).build();
	}

	@GetMapping("/findAllTables")
	public ResultEntity findAllTables() {
		return resultEntityBuilder().payload(tableService.findAllTables()).build();
	}

	@GetMapping("/findTablesByUserId/{userId}")
	public ResultEntity findTablesByUserId(@PathVariable Integer userId) {
		return resultEntityBuilder().payload(tableService.findTablesByUserId(userId)).build();
	}

	@GetMapping("/findActiveTablesByDsId/{dsId}")
	public ResultEntity findTablesByDsId(@PathVariable Integer dsId) {
		return resultEntityBuilder().payload(tableService.findActiveTablesByDsId(dsId)).build();
	}

	@GetMapping("/findActiveTablesBySchemaId/{schemaId}")
	public ResultEntity findActiveTablesBySchemaId(@PathVariable Integer schemaId) {
		return resultEntityBuilder().payload(tableService.findActiveTablesBySchemaId(schemaId)).build();
	}

	/**
	 * 根据id更新table
	 *
	 * @param
	 * @param
	 * @return 更新状态
	 */
	@PostMapping(path = "/startOrStopTableByTableIds")
	public ResultEntity startOrStopTableByTableIds(@RequestBody ArrayList<Integer> ids, String status) {
		try {
			tableService.startOrStopTableByTableIds(ids, status);
			return resultEntityBuilder().build();
		} catch (Exception e) {
			logger.error("Error encountered while startOrStopTableByTableIds Table .", e);
			return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
		}
	}
	/**
	 * 根据tableId获取datasource信息
	 *
	 * @param
	 * @param
	 * @return datasource信息
	 */
	@PostMapping(path = "/getDataSourcesByTableIds")
	public ResultEntity getDataSourcesByTableIds(@RequestBody ArrayList<Integer> ids) {
		try {
			return resultEntityBuilder().payload(tableService.getDataSourcesByTableIds(ids)).build();
		} catch (Exception e) {
			logger.error("Error encountered while getDataSourcesByTableIds Table .", e);
			return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
		}
	}

	@PostMapping(path = "/importRulesByTableId/{tableId}")
	public ResultEntity importRulesByTableId(@PathVariable Integer tableId, @RequestBody String json) {
		try {
			tableService.importRulesByTableId(tableId, json);
			return resultEntityBuilder().build();
		} catch (Exception e) {
			logger.error("Error encountered while importRulesByTableId Table .", e);
			return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
		}
	}

	@GetMapping("/exportRulesByTableId/{tableId}")
	public ResultEntity exportRulesByTableId(@PathVariable Integer tableId) {
		try {
			return resultEntityBuilder().payload(tableService.exportRulesByTableId(tableId)).build();
		} catch (Exception e) {
			logger.error("Exception encountered while request exportRulesByTableId.", e);
			return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
		}
	}

	@PostMapping(path = "/searchTableByIds")
	public ResultEntity searchTableByIds(@RequestBody ArrayList<Integer> ids) {
		try {
			return resultEntityBuilder().payload(tableService.searchTableByIds(ids)).build();
		} catch (Exception e) {
			logger.error("Error encountered while searchTableByIds Table .", e);
			return resultEntityBuilder().status(MessageCode.EXCEPTION).build();
		}
	}

}





