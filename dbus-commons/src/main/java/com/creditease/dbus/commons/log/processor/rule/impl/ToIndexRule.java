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
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.creditease.dbus.commons.log.processor.parse.ParseResult;
import com.creditease.dbus.commons.log.processor.parse.ParseRuleGrammar;
import com.creditease.dbus.commons.log.processor.parse.RuleGrammar;
import com.creditease.dbus.commons.log.processor.rule.IRule;

import java.util.*;

public class ToIndexRule implements IRule {

    public List<List<String>> transform(List<List<String>> datas, List<RuleGrammar> grammar, Rules ruleType) throws Exception {
        if (datas == null || datas.isEmpty())
            return new ArrayList<>();

        List<List<String>> retVal = new ArrayList<>();
        for (List<String> data : datas) {
            Map<String, Object> recordMap = JSON.parseObject(data.get(0), HashMap.class);
            List<ParseResult> prList = ParseRuleGrammar.parse(grammar, data.size(), ruleType);
            TreeMap<Integer, String> tm = new TreeMap();
            for (ParseResult pr : prList) {
                if (recordMap.get(pr.getParamter()) == null) {
                    tm.put(pr.getScope().get(0), "");
                } else {
                    if (recordMap.get(pr.getParamter()) instanceof String) {
                        tm.put(pr.getScope().get(0), (String) recordMap.get(pr.getParamter()));
                    } else if (recordMap.get(pr.getParamter()) instanceof JSONArray ||
                            recordMap.get(pr.getParamter()) instanceof JSONObject) {
                        tm.put(pr.getScope().get(0), JSON.toJSONString(recordMap.get(pr.getParamter())));
                    } else {
                        tm.put(pr.getScope().get(0), JSON.toJSONString(recordMap.get(pr.getParamter())));
                    }
                }

            }
            retVal.add(new LinkedList<>(tm.values()));
        }
        return retVal;
    }

    public static void main(String[] args) {
        List<String> ret = new LinkedList<>();
        String str = "{ \"name\": \"hahaha\", \"sex\": \"man\" }";
        Map<String, String> recordMap = JSON.parseObject(str, HashMap.class);
        TreeMap<Integer, String> tm = new TreeMap();
        tm.put(0, "0");
        tm.put(1, "1");
        tm.put(2, "2");
        tm.put(3, "3");
        tm.put(4, "4");
        tm.put(5, "5");
        tm.put(6, "6");
        tm.put(7, "7");
        tm.put(8, "8");
        tm.put(9, "9");
        tm.put(10, "10");
        tm.values();


        for (Map.Entry<Integer, String> m : tm.entrySet()) {
            ret.add(recordMap.get(m.getValue()));
        }

        int a = 1;
        Object b = JSON.toJSONString(a);
        System.out.println();

    }
}
