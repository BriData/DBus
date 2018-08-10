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

import com.alibaba.fastjson.JSON;
import com.creditease.dbus.domain.mapper.DataSchemaMapper;
import com.creditease.dbus.domain.mapper.DataSourceMapper;
import com.creditease.dbus.domain.mapper.DataTableMapper;
import com.creditease.dbus.domain.model.DataSchema;
import com.creditease.dbus.domain.model.DataSource;
import com.creditease.dbus.domain.model.DataTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.List;

@Component
public class EncodeScheduledTask {

	private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private DataSourceMapper dataSourceMapper;

    @Autowired
    private DataSchemaMapper dataSchemaMapper;

    @Autowired
    private DataTableMapper tableMapper;

    @Scheduled(cron = "* * */6 * * *")
	private void updateEncode() {

        /**
         * 获取管理库中所有数据源信息
         */
        RestTemplate restTemplate = new RestTemplate();
        List<DataSource> dataSources = dataSourceMapper.selectAll();
        dataSources.forEach((DataSource ds) -> {
            List<DataSchema> dataSchemas = dataSchemaMapper.searchSchema(null, Long.valueOf(ds.getId()), null);
            dataSchemas.forEach((DataSchema schema) -> {
                List<DataTable> tables = tableMapper.findBySchemaID(schema.getId());
//                tables.forEach(table -> System.out.println(JSON.toJSONString(table)));
            });
        });
	}
}
