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
import org.apache.commons.lang3.math.NumberUtils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class SubStringRule implements IRule {

    public List<List<String>> transform(List<List<String>> datas, List<RuleGrammar> grammar, Rules ruleType) throws Exception {
        List<List<String>> retVal = new ArrayList<>();
        for (List<String> data : datas) {
            List<String> row = new LinkedList<>(data);
            List<ParseResult> prList = ParseRuleGrammar.parse(grammar, data.size(), ruleType);
            for (ParseResult pr : prList) {
                for (int col : pr.getScope()) {
                    if (col >= data.size())
                        continue;
                    String value = data.get(col);
                    row.remove(col);
                    if (StringUtils.equals(pr.getStartType(), Constants.RULE_TYPE_INDEX) &&
                            StringUtils.equals(pr.getEndType(), Constants.RULE_TYPE_INDEX)) {
                        if (pr.getEnd() != null) {
                            row.add(col, StringUtils.substring(value, NumberUtils.toInt(pr.getStart()), NumberUtils.toInt(pr.getEnd())));
                        } else {
                            row.add(col, StringUtils.substring(value, NumberUtils.toInt(pr.getStart())));
                        }
                    } else if (StringUtils.equals(pr.getStartType(), Constants.RULE_TYPE_INDEX) &&
                            StringUtils.equals(pr.getEndType(), Constants.RULE_TYPE_STRING)) {
                        row.add(col, StringUtils.substring(value, NumberUtils.toInt(pr.getStart()),
                                StringUtils.lastIndexOf(value, StringUtils.defaultString(pr.getEnd(), "")) + StringUtils.defaultString(pr.getEnd(), "").length()));
                    } else if (StringUtils.equals(pr.getStartType(), Constants.RULE_TYPE_STRING) &&
                            StringUtils.equals(pr.getEndType(), Constants.RULE_TYPE_INDEX)) {
                        if (pr.getEnd() != null) {
                            row.add(col, StringUtils.substring(value,
                                    StringUtils.indexOf(value, pr.getStart()), NumberUtils.toInt(pr.getEnd())));
                        } else {
                            row.add(col, StringUtils.substring(value,
                                    StringUtils.indexOf(value, pr.getStart())));
                        }
                    } else if (StringUtils.equals(pr.getStartType(), Constants.RULE_TYPE_STRING) &&
                            StringUtils.equals(pr.getEndType(), Constants.RULE_TYPE_STRING)) {
                        if (pr.getEnd() != null) {
                            row.add(col, StringUtils.substring(value, StringUtils.indexOf(value, pr.getStart()), StringUtils.lastIndexOf(value, pr.getEnd()) + pr.getEnd().length()));
                        } else {
                            row.add(col, StringUtils.substring(value, StringUtils.indexOf(value, pr.getStart())));
                        }
                    }
                }
            }
            retVal.add(row);
        }
        return retVal;
    }

    public static void main(String[] args) {
        String str = "KafkaConsumerEvent$1 299 - stat信息发送到topic:dbus_statistic, key:data_increment_heartbeat.oracle.db4.AMQUE.T_LOAN_INFO.5.0.0.1512024836812|1512024835870|ok.wh_placeholder, offset:716250631";
        String str1 = "KafkaConsumerEvent$1 299 - stat信息发送到topic:dbus_statistic";
        // 1. string string
        String start = "topic";
        String end = "dbus_statistic";
        System.out.println(StringUtils.substring(str, StringUtils.indexOf(str1, start), StringUtils.lastIndexOf(str1, end) + end.length()));
        // 2. string index
//        start = "aaa";
//        end = "23";
//        System.out.println(StringUtils.substring(str, StringUtils.indexOf(str, start), NumberUtils.toInt(end)));
//        // 3. index index
//        start = "3";
//        end = "23";
//        System.out.println(StringUtils.substring(str, NumberUtils.toInt(start), NumberUtils.toInt(end)));
//        // 4. index string
//        start = "3";
//        end = "ccc";
//        System.out.println(StringUtils.substring(str, NumberUtils.toInt(start),
//                StringUtils.lastIndexOf(str, end) + end.length()));
    }
}
