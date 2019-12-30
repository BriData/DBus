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
import java.util.List;
import java.util.regex.Pattern;


public class FilterRule implements IRule {

    public List<List<String>> transform(List<List<String>> datas, List<RuleGrammar> grammar, Rules ruleType) throws Exception {
        List<List<String>> retVal = new ArrayList<>();
        for (List<String> data : datas) {
            List<ParseResult> prList = ParseRuleGrammar.parse(grammar, data.size(), ruleType);
            boolean isOk = false;
            for (ParseResult pr : prList) {
                List<Integer> scope = pr.getScope();
                for (int col : scope) {
                    String val = StringUtils.EMPTY;
                    if (col < data.size())
                        val = data.get(col);

                    if (StringUtils.equals(pr.getRuleType(), Constants.RULE_TYPE_STRING)) {
                        if (pr.getEq()) {
                            isOk = StringUtils.contains(val, pr.getParamter());
                        } else {
                            isOk = !StringUtils.contains(val, pr.getParamter());
                        }
                    } else if (StringUtils.equals(pr.getRuleType(), Constants.RULE_TYPE_REGEX)) {
                        if (pr.getEq()) {
                            isOk = Pattern.compile(pr.getParamter()).matcher(val).find();
                        } else {
                            isOk = !Pattern.compile(pr.getParamter()).matcher(val).find();
                        }
                    }
                }
            }
            if (isOk)
                retVal.add(data);
        }
        return retVal;
    }


    public static void main(String[] args) {
        System.out.println(Pattern.compile("[A-Z]").matcher("ABCDEF").find());
        System.out.println(Pattern.compile("\\d{3}").matcher("T123aaa456").find());
    }
}
