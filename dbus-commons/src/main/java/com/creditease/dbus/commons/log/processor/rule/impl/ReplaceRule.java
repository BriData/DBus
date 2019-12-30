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


package com.creditease.dbus.commons.log.processor.rule.impl;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.log.processor.parse.ParseResult;
import com.creditease.dbus.commons.log.processor.parse.ParseRuleGrammar;
import com.creditease.dbus.commons.log.processor.parse.RuleGrammar;
import com.creditease.dbus.commons.log.processor.rule.IRule;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ReplaceRule implements IRule {

    public List<List<String>> transform(List<List<String>> datas, List<RuleGrammar> grammar, Rules ruleType) throws Exception {
        List<List<String>> retVal = new ArrayList<>();
        for (List<String> data : datas) {
            List<ParseResult> prList = ParseRuleGrammar.parse(grammar, data.size(), ruleType);
            List<String> row = new LinkedList<>(data);
            for (ParseResult pr : prList) {
                for (int col : pr.getScope()) {
                    if (col >= row.size())
                        continue;
                    String colData = row.get(col);
                    row.remove(col);
                    if (StringUtils.equals(pr.getRuleType(), Constants.RULE_TYPE_STRING)) {
                        row.add(col, StringUtils.replace(colData, pr.getParamter(), StringUtils.defaultString(pr.getOperate(), "")));
                    } else {
                        //正则表达式替换: colData: 被替换字符串, pr.getParamter(): 正则表达式, pr.getOperate():替换字符换
                        row.add(col, StringUtils.replaceAll(colData, pr.getParamter(), StringUtils.defaultString(pr.getOperate(), "")));
                    }
                }
            }
            retVal.add(row);
        }
        return retVal;
    }

    public static void main(String[] args) {
        String str = "\tCheckHeartBeatEvent 196 - 节点:/DBus/HeartBeat/Monitor/new_ds_zhong/new_ds_zhong_schema/t1,状态:异常,报警次数:1,超时次数:3";
        String str2 = "\tCheckHeartBeatEvent 196 - 节点:/DBus/HeartBeat/Monitor/new_ds_zhong/new_ds_zhong_schema/t1,状态:异常,报警次数:1,超时次数:3";
        String str3 = "HeartBeatDaoImpl 91 - [db-HeartBeatDao] 数据源: cedb, 插入心跳包成功. {\"node\":\"/DBus/HeartBeat/Monitor/cedb/DBUS/DB_FULL_PULL_REQUESTS\",\"time\":1510022118907,\"type\":\"checkpoint\",\"txTime\":1510021478425}";
        //正则表达式替换
        System.out.println(StringUtils.replaceAll(str3, "", "AB"));
        //字符串替换
        System.out.println(StringUtils.replace("aaaabbcc[a-z][a-z]ecskf", "[a-z]", "AB"));
    }
}
