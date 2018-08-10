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

package com.creditease.dbus.commons.log.processor.rule.impl;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.log.processor.parse.ParseResult;
import com.creditease.dbus.commons.log.processor.parse.ParseRuleGrammar;
import com.creditease.dbus.commons.log.processor.parse.RuleGrammar;
import com.creditease.dbus.commons.log.processor.rule.IRule;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

public class SplitRule implements IRule {

    public List<String> transform(List<String> data, List<RuleGrammar> grammar, Rules ruleType) throws Exception{
        List<String> ret = new LinkedList<>(data);
        List<ParseResult> prList = ParseRuleGrammar.parse(grammar, data.size(), ruleType);
        for (ParseResult pr : prList) {
            for(int col : pr.getScope()) {
                if (col >= data.size()) continue;
                String value = data.get(col);
                ret.remove(col);
                if(StringUtils.equals(pr.getRuleType(), Constants.RULE_TYPE_STRING)) {
                    ret.addAll(col, Arrays.asList(StringUtils.splitByWholeSeparator(value, pr.getParamter())));
                } else {
                    ret.addAll(col, Arrays.asList(value.split(pr.getParamter())));
                }
            }
        }
        return ret;
    }


    public static void main(String[] args) {
        String value = "bbb123aaa456aaccc";
        String value2 = "bbb123aaa2456aaccc";
        System.out.println(Arrays.asList(value.split("\\d{3}")));
        System.out.println(Arrays.asList(value2.split("23")));
        System.out.println(Arrays.asList(StringUtils.splitByWholeSeparator(value2, "23")));
    }
}
