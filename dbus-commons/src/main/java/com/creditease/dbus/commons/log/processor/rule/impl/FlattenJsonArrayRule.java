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
import com.alibaba.fastjson.JSONPath;
import com.creditease.dbus.commons.log.processor.parse.ParseResult;
import com.creditease.dbus.commons.log.processor.parse.ParseRuleGrammar;
import com.creditease.dbus.commons.log.processor.parse.RuleGrammar;
import com.creditease.dbus.commons.log.processor.rule.IRule;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by mal on 2019/3/6.
 */
public class FlattenJsonArrayRule implements IRule {

    @Override
    public List<List<String>> transform(List<List<String>> datas, List<RuleGrammar> grammar, Rules ruleType) throws Exception {
        List<List<String>> retVal = new ArrayList<>();
        for (List<String> data : datas) {
            List<ParseResult> prList = ParseRuleGrammar.parse(grammar, data.size(), ruleType);
            for (ParseResult pr : prList) {
                for (int col : pr.getScope()) {
                    if (col >= data.size())
                        continue;
                    String wkVal = data.get(col);
                    try {
                        List<String> flattenList = null;
                        if (StringUtils.isNoneEmpty(pr.getParamter())) {
                            flattenList = flattenJsonArray(wkVal, pr.getParamter());
                        } else {
                            flattenList = flattenArray(wkVal);
                        }
                        if (flattenList != null && !flattenList.isEmpty()) {
                            for (String val : flattenList) {
                                List<String> row = new LinkedList<>(data);
                                row.remove(col);
                                row.add(col, val);
                                retVal.add(row);
                            }
                        }
                    } catch (Exception e) {
                        break;
                    }
                }
            }
        }
        return retVal;
    }

    public static void main(String[] args) throws Exception {
        String wkVal = "[[1,2,3],[\"a\",\"b\",\"c\"]]";
        String path = "a.aa";

        List<List<String>> datas = new ArrayList<>();
        List<String> row = new ArrayList<>();
        row.add("flatten json array test");
        row.add(wkVal);
        datas.add(row);

        List<RuleGrammar> grammar = new ArrayList<>();
        RuleGrammar rg = new RuleGrammar();
        rg.setRuleScope("1");
        rg.setRuleParamter("");
        grammar.add(rg);

        print(datas);

        Rules rule = Rules.fromStr("flattenJsonArray");
        List<List<String>> retVal = rule.getRule().transform(datas, grammar, rule);
        print(retVal);

        grammar = new ArrayList<>();
        rg = new RuleGrammar();
        rg.setRuleScope("1");
        rg.setRuleParamter("");
        grammar.add(rg);
        retVal = rule.getRule().transform(retVal, grammar, rule);
        print(retVal);

    }

    private static void print(List<List<String>> retVal) {
        for (List<String> data : retVal) {
            for (String item : data) {
                System.out.print(item + " | ");
            }
            System.out.println();
        }
    }

    private static List<String> flattenArray(String wkVal) {
        List<String> rows = new ArrayList<>();
        if (StringUtils.isNoneEmpty(wkVal)) {
            JSONArray jsonArray = JSON.parseArray(wkVal);
            for (int i = 0; i < jsonArray.size(); i++) {
                Object item = jsonArray.get(i);
                if (item != null) {
                    rows.add(jsonArray.get(i).toString());
                } else {
                    rows.add(StringUtils.EMPTY);
                }
            }
        }
        return rows;
    }

    private static List<String> flattenJsonArray(String wkVal, String path) {
        List<String> rows = new ArrayList<>();
        JSONObject json = JSONObject.parseObject(wkVal);
        String parentPath = StringUtils.substringBeforeLast(path, ".");
        String currentPath = StringUtils.substringAfterLast(path, ".");
        if (json.containsKey(path)) {
            parentPath = StringUtils.EMPTY;
            currentPath = path;
        }
        Object currentJsonObj = JSONPath.read(wkVal, path);
        Object parentJsonObj = null;
        if (StringUtils.isBlank(parentPath)) {
            parentJsonObj = json;
        } else {
            parentJsonObj = JSONPath.read(wkVal, parentPath);
        }

        if (currentJsonObj instanceof JSONArray) {
            JSONArray retJsonArray = new JSONArray();
            JSONArray currentJsonArray = (JSONArray) currentJsonObj;
            for (int i = 0; i < currentJsonArray.size(); i++) {
                JSONObject jsonItemWk = new JSONObject();
                if (parentJsonObj instanceof JSONObject) {
                    jsonItemWk = JSONObject.parseObject(((JSONObject) parentJsonObj).toJSONString());
                    jsonItemWk.remove(currentPath);
                }
                jsonItemWk.put(currentPath, currentJsonArray.get(i));
                retJsonArray.add(jsonItemWk);
            }
            if (json.containsKey(path)) {
                for (int i = 0; i < retJsonArray.size(); i++) {
                    JSONObject item = (JSONObject) retJsonArray.get(i);
                    rows.add(item.toJSONString());
                }
            } else {
                JSONPath.set(json, parentPath, retJsonArray);
                rows.add(json.toJSONString());
            }
        }
        return rows;
    }

}
