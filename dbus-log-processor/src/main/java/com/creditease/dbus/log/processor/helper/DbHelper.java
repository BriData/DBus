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


package com.creditease.dbus.log.processor.helper;

import com.creditease.dbus.commons.log.processor.rule.impl.Rules;
import com.creditease.dbus.log.processor.dao.IProcessorLogDao;
import com.creditease.dbus.log.processor.dao.impl.ProcessorLogDao;
import com.creditease.dbus.log.processor.util.Constants;
import com.creditease.dbus.log.processor.vo.DBusDataSource;
import com.creditease.dbus.log.processor.vo.RuleInfo;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class DbHelper {

    private static Logger logger = LoggerFactory.getLogger(DbHelper.class);

    private DbHelper() {
    }

    public static DBusDataSource loadDBusDataSourceConf(String dsName) {
        IProcessorLogDao dao = new ProcessorLogDao();
        return dao.loadDBusDataSourceConf(Constants.DBUS_CONFIG_DB_KEY, dsName);
    }

    public static Map<Long, Map<Long, List<RuleInfo>>> loadRuleInfoConf(String dsName, Map<String, String> activeTableToTopicMap) {
        IProcessorLogDao dao = new ProcessorLogDao();
        List<RuleInfo> ruleInfos = dao.loadActiveTableRuleInfo(Constants.DBUS_CONFIG_DB_KEY, dsName);
        Map<Long, Map<Long, List<RuleInfo>>> ruleTableInfo = new TreeMap<>();

        for (RuleInfo ri : ruleInfos) {
            if (!ruleTableInfo.containsKey(ri.getTableId())) {
                // 当groupId等于-1时，表示只有table没有配置规则
                if (ri.getGroupId() != -1 && ri.getOrderId() != -1) {
                    Map<Long, List<RuleInfo>> ruleGroupInfo = new TreeMap<>();
                    List<RuleInfo> rules = new ArrayList<>();
                    rules.add(ri);
                    ruleGroupInfo.put(ri.getGroupId(), rules);
                    ruleTableInfo.put(ri.getTableId(), ruleGroupInfo);
                }
            } else {
                if (ruleTableInfo.get(ri.getTableId()).containsKey(ri.getGroupId())) {
                    ruleTableInfo.get(ri.getTableId()).get(ri.getGroupId()).add(ri);
                } else if (ri.getOrderId() != -1) {
                    List<RuleInfo> rules = new ArrayList<>();
                    rules.add(ri);
                    ruleTableInfo.get(ri.getTableId()).put(ri.getGroupId(), rules);
                }
            }
            String tableNameSpace = StringUtils.joinWith("|", ri.getDsName(), ri.getSchemaName(), ri.getTableName(), ri.getVersion());
            if (!activeTableToTopicMap.containsKey(tableNameSpace)) {
                activeTableToTopicMap.put(tableNameSpace, ri.getOutputTopic());
                logger.info("activeTableToTopicMap key: {}, value: {}", tableNameSpace, ri.getOutputTopic());
            }
        }

        for (Map.Entry<Long, Map<Long, List<RuleInfo>>> tableGroupRules : ruleTableInfo.entrySet()) {
            for (Map.Entry<Long, List<RuleInfo>> groupRules : tableGroupRules.getValue().entrySet()) {
                if (groupRules.getValue() != null &&
                        !groupRules.getValue().isEmpty()) {
                    RuleInfo lastElem = groupRules.getValue().get(groupRules.getValue().size() - 1);
                    Rules ruleType = Rules.fromStr(lastElem.getRuleTypeName());
                    if (ruleType != Rules.SAVEAS) {
                        RuleInfo asRi = new RuleInfo();
                        asRi.setRuleTypeName("saveAs");
                        asRi.setRuleGrammar("[]");
                        groupRules.getValue().add(asRi);
                    }
                }
            }
        }
        return ruleTableInfo;
    }

    public static void loadAbortTable(String dsName, Map<String, String> abortTableToTopicMap) {
        IProcessorLogDao dao = new ProcessorLogDao();
        List<RuleInfo> ruleInfos = dao.loadAbortTableRuleInfo(Constants.DBUS_CONFIG_DB_KEY, dsName);
        for (RuleInfo ri : ruleInfos) {
            String tableNameSpace = StringUtils.joinWith("|", ri.getDsName(), ri.getSchemaName(), ri.getTableName(), ri.getVersion());
            if (!abortTableToTopicMap.containsKey(tableNameSpace)) {
                abortTableToTopicMap.put(tableNameSpace, ri.getOutputTopic());
                logger.info("abortTableToTopicMap key: {}, value: {}", tableNameSpace, ri.getOutputTopic());
            }
        }
    }
}
