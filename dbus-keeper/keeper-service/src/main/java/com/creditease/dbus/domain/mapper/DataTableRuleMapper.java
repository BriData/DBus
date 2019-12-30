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


package com.creditease.dbus.domain.mapper;

import com.creditease.dbus.domain.model.DataTableRule;
import com.creditease.dbus.domain.model.DataTableRuleGroup;
import com.creditease.dbus.domain.model.RuleInfo;
import org.apache.ibatis.annotations.Param;

import java.util.HashMap;
import java.util.List;

public interface DataTableRuleMapper {

    List<DataTableRuleGroup> getAllRuleGroup(Integer tableId);

    List<DataTableRuleGroup> getActiveRuleGroupByTableId(Integer tableId);

    int updateRuleGroup(DataTableRuleGroup dataTableRuleGroup);

    int deleteRuleGroup(Integer groupId);

    int addGroup(DataTableRuleGroup dataTableRuleGroup);

    List<DataTableRule> getAllRules(Integer groupId);

    int deleteRules(Integer groupId);

    int saveAllRules(List<DataTableRule> list);

    List<RuleInfo> getAsRuleInfo(@Param("tableId") Integer tableId, @Param("ruleTypeName") String ruleTypeName);

    int insertRuleGroupVersion(HashMap<String, Object> ruleGroupVersion);

    int insertRulesVersion(HashMap<String, Object> rulesVersion);
}
