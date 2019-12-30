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

import com.creditease.dbus.commons.log.processor.parse.ParseResult;
import com.creditease.dbus.commons.log.processor.parse.ParseRuleGrammar;
import com.creditease.dbus.commons.log.processor.parse.RuleGrammar;
import com.creditease.dbus.commons.log.processor.rule.IRule;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ConcatRule implements IRule {

    public List<List<String>> transform(List<List<String>> datas, List<RuleGrammar> grammar, Rules ruleType) throws Exception {
        List<List<String>> retVal = new ArrayList<>();
        for (List<String> data : datas) {
            List<String> row = new LinkedList<>(data);
            List<ParseResult> prList = ParseRuleGrammar.parse(grammar, data.size(), ruleType);
            //将指定的列合并起来
            for (ParseResult pr : prList) {
                List<String> elems = new LinkedList<>();
                for (int col : pr.getScope()) {
                    if (col >= data.size()) continue;
                    elems.add(data.get(col));
                }
                row.removeAll(elems);
                row.add(pr.getScope().get(0), StringUtils.join(elems, pr.getParamter()));
            }
            retVal.add(row);
        }
        //返回结果
        return retVal;
    }

    public static void main(String[] args) {
        List<String> value = new LinkedList<>();
        List<String> value2 = new LinkedList<>();
        value.add("aa");
        value.add("bb");
        value.add("cc");
        value.add("dd");
        value2.add("aa");
        value2.add("aa");
        value.removeAll(value2);

        List<Integer> scope = new ArrayList<>();
        scope.add(0);
        scope.add(1);
        scope.add(2);
        scope.add(3);

        List<String> elems = new LinkedList<>();
        for (int col : scope) {
            elems.add(col, value.get(col));
        }
        System.out.println(StringUtils.join(elems, "#"));
    }
}
