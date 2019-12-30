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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.log.processor.parse.ParseResult;
import com.creditease.dbus.commons.log.processor.parse.ParseRuleGrammar;
import com.creditease.dbus.commons.log.processor.parse.RuleGrammar;
import com.creditease.dbus.commons.log.processor.rule.IRule;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;


public class FlattenUmsRule implements IRule {

    public List<List<String>> transform(List<List<String>> datas, List<RuleGrammar> grammar, Rules ruleType) throws Exception {
        List<List<String>> retVal = new ArrayList<>();
        for (List<String> data : datas) {
            List<ParseResult> prList = ParseRuleGrammar.parse(grammar, data.size(), ruleType);
            boolean isOk = false;
            JSONObject jsonObject = JSON.parseObject(data.get(0));
            for (ParseResult pr : prList) {
                if (StringUtils.contains(jsonObject.getString("namespace"), pr.getParamter())) {
                    isOk = true;
                }
            }
            if (isOk)
                retVal.add(data);
        }
        return retVal;
    }


    public static void main(String[] args) {
        String str = "{\"ums_op_\":\"i\",\"col_6\":\"2017/10/23 13:27:55.934 \",\"col_5\":\"2017-10-23T05:27:56.408Z\",\"col_4\":\"7476.473\",\"col_3\":\"1508728999461\",\"col_2\":\"1508736475934\",\"col_1\":\"0\",\"ums_ts_\":\"2017-10-23 13:27:55.000\",\"col_0\":\"/DBus/HeartBeat/Monitor/maasoracle/DBUS/DB_HEARTBEAT_MONITOR\",\"namespace\":\"heartbeat_log.dbus.heartbeat_log_table.0.0.0\",\"ums_id_\":\"107107739238931139\",\"ums_uid_\":\"559521\"}";
        JSONObject jsonObject = JSON.parseObject(str);
        if (StringUtils.contains(jsonObject.getString("namespace"), "heartbeat_log.dbus.heartbeat_log_table.0.0.0")) {
            System.out.println(true);
        }
    }
}
