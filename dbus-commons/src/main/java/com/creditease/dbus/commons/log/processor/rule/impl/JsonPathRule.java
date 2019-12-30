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
import com.creditease.dbus.commons.log.processor.parse.ParseResult;
import com.creditease.dbus.commons.log.processor.parse.ParseRuleGrammar;
import com.creditease.dbus.commons.log.processor.parse.RuleGrammar;
import com.creditease.dbus.commons.log.processor.rule.IRule;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class JsonPathRule implements IRule {

    public List<List<String>> transform(List<List<String>> datas, List<RuleGrammar> grammar, Rules ruleType) throws Exception {
        List<List<String>> retVal = new ArrayList<>();
        for (List<String> data : datas) {
            List<String> ret = new LinkedList<>(data);
            List<ParseResult> prList = ParseRuleGrammar.parse(grammar, data.size(), ruleType);
            for (ParseResult pr : prList) {
                for (int col : pr.getScope()) {
                    if (col >= data.size())
                        continue;
                    String field = StringUtils.EMPTY;
                    try {
                        if (JsonPath.parse(data.get(col)).read(pr.getParamter()) instanceof String) {
                            field = JsonPath.parse(data.get(col)).read(pr.getParamter());
                        } else {
                            field = JSON.toJSONString(JsonPath.parse(data.get(col)).read(pr.getParamter()));
                        }
                    } catch (PathNotFoundException e) {
                        field = StringUtils.EMPTY;
                    }
                    ret.add(field);
                }
            }
            retVal.add(ret);
        }
        return retVal;
    }

}
