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

import com.alibaba.fastjson.JSON;
import com.creditease.dbus.commons.log.processor.parse.Field;
import com.creditease.dbus.commons.log.processor.parse.RuleGrammar;
import com.creditease.dbus.commons.log.processor.rule.IRule;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by Administrator on 2017/10/12.
 */
public class SaveAsRule implements IRule {

    public List<String> transform(List<String> data, List<RuleGrammar> grammar, Rules ruleType) throws Exception{
        List<String> ret = new ArrayList<>();
        Map<Integer, List<RuleGrammar>> grammarMap = new HashMap<>();
        for (RuleGrammar rg : grammar) {
            if (StringUtils.isNoneBlank(rg.getRuleScope()) && StringUtils.isNumeric(rg.getRuleScope())) {
                if (!grammarMap.containsKey(Integer.parseInt(rg.getRuleScope()))) {
                    grammarMap.put(Integer.parseInt(rg.getRuleScope()), new ArrayList<>());
                }
                grammarMap.get(Integer.parseInt(rg.getRuleScope())).add(rg);
            }
        }
        for (int idx = 0; idx < grammar.size(); idx++) {
            if (grammarMap.containsKey(idx)) {
                for (RuleGrammar rg : grammarMap.get(idx)) {
                    Field field = new Field();
                    field.setEncoded(false);
                    field.setNullable(false);
                    field.setValue(idx >= data.size() ? "" : data.get(idx));
                    field.setName(rg.getName());
                    field.setType(rg.getType());
                    ret.add(JSON.toJSONString(field));
                }
            }
        }
        return ret;
    }

    public static void main(String[] args) throws Exception {
        List<String> data = new ArrayList<>();
        data.add("a");
        data.add("b");
        data.add("c");
        data.add("d");

        List<RuleGrammar> ruleGrammars = new ArrayList<>();
        for(int i = 0; i < 5; i++)
        {
            RuleGrammar rg = new RuleGrammar();
            rg.setRuleScope(String.valueOf(i));
            rg.setName("col_" + i);
            rg.setType("STRING");
            ruleGrammars.add(rg);
        }


        SaveAsRule saveAsRule = new SaveAsRule();
        List<String> results = saveAsRule.transform(data, ruleGrammars, Rules.SAVEAS);
        System.out.println(results.toString());


    }

}
