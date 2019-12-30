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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class SplitRule implements IRule {

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
                    if (StringUtils.equals(pr.getRuleType(), Constants.RULE_TYPE_STRING)) {
                        row.addAll(col, Arrays.asList(StringUtils.splitByWholeSeparatorPreserveAllTokens(value, pr.getParamter())));
                    } else {
                        row.addAll(col, Arrays.asList(value.split(pr.getParamter())));
                    }
                }
            }
            retVal.add(row);
        }
        return retVal;
    }


    public static void main(String[] args) {
        String value2 = "2019-04-25 17:40:42:384|1556185242373_224_14|1|征信报告解析|9002||0012444|{\"crsType\":\"0001\",\"bsclientId\":\"126526254\",\"channelId\":\"0001\",\"bsapplyId\":\"36525695\",\"IdNumber\":\"412326198010067570\",\"signString\":\"Yb4F65DcXVb8fFnVxSF9cKQ9OFJiDb5y3sfU4VBvJxC8jIlaGBlQj1UK+jFC/SECbvIN4w0LsrdmR2lzwgmRWyrp7/jIFxgVMq45LgF70SKDZ2c/sDlnAY2Jmbl4FM1A/zv1B/JHVqNvzSZNfFPbK90Li1XNoZ9Mwou6HL4b2GY=\",\"custName\":\"李宁宁\",\"notifyUrl\":\"http://localhost:8080/beehive/creditReportController/getCreditReportInfoNotice\",\"beginDate\":\"20190401\"}";
//        System.out.println(Arrays.asList(value.split("\\d{3}")));
//        System.out.println(Arrays.asList(value2.split("23")));
        System.out.println(Arrays.asList(StringUtils.splitByWholeSeparator(value2, "|")));
        System.out.println(Arrays.asList(StringUtils.splitByWholeSeparatorPreserveAllTokens(value2, "|")));
    }
}
