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

import com.creditease.dbus.bean.SchemaAndTablesInfoBean;
import com.creditease.dbus.domain.mapper.DataSchemaMapper;
import com.creditease.dbus.domain.mapper.DataTableMapper;
import com.creditease.dbus.domain.model.DataSchema;
import com.creditease.dbus.domain.model.DataSource;
import com.creditease.dbus.domain.model.DataTable;
import com.creditease.dbus.enums.DbusDatasourceType;
import com.creditease.dbus.service.schema.MongoSchemaFetcher;
import com.creditease.dbus.service.schema.SchemaFetcher;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * User: 王少楠
 * Date: 2018-05-07
 * Time: 下午5:34
 */
@Service
public class DataSchemaService {
	@Autowired
	private DataSchemaMapper mapper;

	@Autowired
	private DataTableMapper tableMapper;

	@Autowired
	private TableService dataTableService;

	@Autowired
	private DataSourceService dataSourceService;

	private static Logger logger = LoggerFactory.getLogger(DataSchemaService.class);
	/* 方法中用到的高频字符串常量*/
	public static final String DBUS = "dbus";
	public static final String RESULT = "result";

	public static final String MYSQL = "mysql";
	public static final String ORACLE = "oracle";

	/**
	 * schema索
	 *
	 * @return
	 */
	public PageInfo<Map<String, Object>> searchSchemaAndDs(int pageNum, int pageSize, Integer dsId, String schemaName) {
		Map<String, Object> param = new HashedMap();
		param.put("dsId", dsId);
		param.put("schemaName", schemaName == null ? schemaName : schemaName.trim());
		PageHelper.startPage(pageNum, pageSize);
		List<Map<String, Object>> dataSchemas = mapper.searchSchemaAndDs(param);
		return new PageInfo<>(dataSchemas);
	}

	/**
	 * @param schemaId schema 的id
	 *                 dsId schema所属ds的id
	 *                 schemaName schema名称
	 * @param dsId
	 */
	public List<DataSchema> searchSchema(Long schemaId, Long dsId, String schemaName) {
		return mapper.searchSchema(schemaId, dsId, schemaName == null ? schemaName : schemaName.trim());
	}

	/**
	 * 根据 ID更新某条记录
	 *
	 * @param updateOne
	 */
	public int update(DataSchema updateOne) {
		// updateOne.setCreateTime(new Date());
		if (updateOne.getStatus().equals("inactive")) {
			tableMapper.inactiveTableBySchemaId(updateOne.getId());
		}
		return mapper.update(updateOne);
	}

	/**
	 * @return 插入的新数据的ID
	 */
	public Integer insertOne(DataSchema newOne) {
		newOne.setCreateTime(new Timestamp(System.currentTimeMillis()));
		mapper.insert(newOne);
		return newOne.getId();
	}

	public int modifyDataSchemaStatus(Map<String, Object> param) {
		return mapper.updateSchemaStatusByPrimaryKey(param);
	}

	public int deleteBySchemaId(Long id) {
		return mapper.deleteBySchemaId(id);
	}

	public DataSchema selectById(Long id) {
		return mapper.selectById(id);
	}

	/**
	 * @return 根据dsType，构造不同的dbus schema
	 */
	private DataSchema getDbusSchema(String dsType, Integer dsId, String dsName) {
		DataSchema dbusSchema = new DataSchema();

		StringBuilder srcTopic = new StringBuilder(dsName);
		StringBuilder targetTopic = new StringBuilder(dsName);
		if (MYSQL.equals(dsType)) {
			srcTopic.append(".").append(DBUS);
			targetTopic.append(".").append(DBUS).append(".").append(RESULT);

			dbusSchema.setSchemaName(DBUS);
		} else {
			srcTopic.append(".").append(DBUS.toUpperCase());
			targetTopic.append(".").append(DBUS.toUpperCase()).append(".").append(RESULT);

			dbusSchema.setSchemaName(DBUS.toUpperCase());
		}
		dbusSchema.setDsId(dsId);
		dbusSchema.setStatus(DataSchema.ACTIVE);
		dbusSchema.setDescription("");
		dbusSchema.setSrcTopic(srcTopic.toString());
		dbusSchema.setTargetTopic(targetTopic.toString());

		return dbusSchema;
	}

	/**
	 * 插入新的schema, 根据dsId和schemaName过滤
	 *
	 * @return 新的schemaId 或者 其他信息
	 */
	private Integer insertSchema(DataSchema newSchema) {
		Integer dsId = newSchema.getDsId();
		String schemaName = newSchema.getSchemaName();
		if (dsId == null || StringUtils.isEmpty(schemaName)) {
			logger.error("[insert schema] param illegal: dsId:{},schemaName:{}",
					dsId, schemaName);
			return -1;
		}
		// 如果不存在，插入
		DataSchema oldSchema = findSchema(dsId, schemaName);
		if (oldSchema == null) {
			return insertOne(newSchema);
		} else {
			return oldSchema.getId();
		}
	}

	/**
	 * 根据dsId和schemaName 查找schema
	 *
	 * @param dsId
	 * @param schemaName
	 * @return
	 */
	public DataSchema findSchema(int dsId, String schemaName) {
		return mapper.findByDsIdAndSchemaName(dsId, StringUtils.trim(schemaName));
	}

	public void addSchemaAndTables(List<SchemaAndTablesInfoBean> schemaAndTablesList) throws Exception {
		for (SchemaAndTablesInfoBean schemaAndTables : schemaAndTablesList) {
			addSchemaAndTables(schemaAndTables);
		}

	}

	public void addSchemaAndTables(SchemaAndTablesInfoBean schemaAndTables) throws Exception {
		try {
			//获取传入的具体信息
			DataSchema newSchema = schemaAndTables.getSchema();
			newSchema.setStatus(DataSchema.ACTIVE); //添加默认的状态，active
			String schemaName = newSchema.getSchemaName();
			if (StringUtils.isEmpty(schemaName)) {
				logger.error("[add schema and tables] schemaName is empty. schemaInfo : {}", newSchema.toString());
				throw new IllegalArgumentException("schemaName is illegal");
			}
			Integer dsId = newSchema.getDsId();
			String dsType = newSchema.getDsType();
			String dsName = newSchema.getDsName();

			List<DataTable> newTables = schemaAndTables.getTables();
			//插入schema, 并获得插入的schemaId, 下面插入table用
			Integer schemaId = insertSchema(newSchema);
			if (schemaId < 0) {
				logger.error("[add schema and tables] Insert schema error: : {}", newSchema.toString());
				throw new IllegalArgumentException("insert schema error");
			}
			logger.info("insert schema [{}] success.", newSchema.getSchemaName());

			//构造默认的dbus schema,并尝试插入（存在不插入）
			DataSchema dbusSchema = getDbusSchema(dsType, dsId, dsName);
			Integer dbusSchemaId = insertSchema(dbusSchema);
			if (dbusSchemaId < 0) {
				logger.error("[add schema and tables] Insert dbus schema error: " +
						"dbus schema info's format is illegal. dbusSchema:{}", dbusSchema.toString());
				throw new IllegalArgumentException("insert dbus schema error");
			}
			//构造默认需要插入的表。
			// mysql和oracle添加，需要对默认表检查插入
			//log类型的不需要
			List<DataTable> defaultTables;
			if (StringUtils.equals(MYSQL, dsType)) {
				//！！！ 注意dbusSchemaId和schemaId的使用处
				defaultTables = dataTableService.getDefaultTableForMySQL(dsId, dsName, dbusSchemaId);
			} else if (StringUtils.equals(ORACLE, dsType)) {
				defaultTables = dataTableService.getDefaultTableForNotMySQL(dsId, dsName, dbusSchemaId);
			}
			else {
				defaultTables = new ArrayList<>();
			}
			//构造需要插入的表列表，然后将需要插入的表加入
			List<DataTable> tablesToAdd = defaultTables;

			//将dsId和schemaId等信息加入table中
			for (int i = 0; i < newTables.size(); i++) {
				DataTable newTable = newTables.get(i);
				String tableName = newTable.getTableName();
				// 如果是默认的表，略过（列表中已存在）
				if (dataTableService.ifDefaultTable(tableName)) {
					continue;
				}
				newTable.setDsId(dsId);
				newTable.setDsType(dsType);
				newTable.setDsName(dsName);
				newTable.setSchemaId(schemaId);
				newTable.setSchemaName(schemaName);
				newTable.setStatus("ok");
				tablesToAdd.add(newTable);
				logger.info("newTable [{}] added.", newTable.getTableName());
			}

			//插入表
			Integer tableInsertResult = dataTableService.insertManageTable(tablesToAdd);
			if (tableInsertResult == -1) {
				throw new Exception("tables insert error!");
			}
		} catch (Exception e) {
			logger.error("[add schema and tables] Exception:{}", e);
			throw e;
		}
	}

	public List<DataSchema> fetchSchemas(Integer dsId) throws Exception {

		DataSource ds = dataSourceService.getById(dsId);
		List<DataSchema> list;
		if (DbusDatasourceType.stringEqual(ds.getDsType(), DbusDatasourceType.MYSQL)
				|| DbusDatasourceType.stringEqual(ds.getDsType(), DbusDatasourceType.ORACLE)
				) {
			SchemaFetcher fetcher = SchemaFetcher.getFetcher(ds);
			list = fetcher.fetchSchema();
		} else if (DbusDatasourceType.stringEqual(ds.getDsType(), DbusDatasourceType.MONGO)) {
			MongoSchemaFetcher fetcher = new MongoSchemaFetcher(ds);
			list = fetcher.fetchSchema();
		} else {
			throw new IllegalArgumentException("Unsupported datasource type");
		}
		for (int i = 0; i < list.size(); i++) {
			list.get(i).setDsId(ds.getId());
			list.get(i).setStatus(ds.getStatus());
			list.get(i).setSrcTopic(ds.getDsName() + "." + list.get(i).getSchemaName());
			list.get(i).setTargetTopic(ds.getDsName() + "." + list.get(i).getSchemaName() + ".result");
		}
		return list;
	}
}
