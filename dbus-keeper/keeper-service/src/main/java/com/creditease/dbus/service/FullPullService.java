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

import com.creditease.dbus.constant.MessageCode;
import com.creditease.dbus.domain.mapper.ProjectTopoTableMapper;
import com.creditease.dbus.domain.model.DataSource;
import com.creditease.dbus.domain.model.DataTable;
import com.creditease.dbus.domain.model.ProjectTopoTable;
import com.creditease.dbus.service.explain.SqlExplainFetcher;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


/**
 * Created by xiancangao on 2018/04/27.
 */
@Service
public class FullPullService {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private TableService tableService;
    @Autowired
    private DataSourceService dataSourceService;
    @Autowired
    private ProjectTopoTableMapper mapper;

    public int updateFullpullCondition(ProjectTopoTable projectTopoTable) throws Exception {
        String codition = projectTopoTable.getFullpullCondition();
        DataTable dataTable = tableService.getById(projectTopoTable.getTableId());
        DataSource dataSource = dataSourceService.getById(dataTable.getDsId());
        SqlExplainFetcher sqlExplainFetcher = SqlExplainFetcher.getFetcher(dataSource);
        boolean result = true;
        if (StringUtils.isNotBlank(codition)) {
            result = sqlExplainFetcher.sqlExplain(dataTable, codition);
        }
        if (result) {
            mapper.updateByPrimaryKey(projectTopoTable);
        } else {
            return MessageCode.FULL_PULL_CODITION_ERROR;
        }
        return 0;
    }
}
