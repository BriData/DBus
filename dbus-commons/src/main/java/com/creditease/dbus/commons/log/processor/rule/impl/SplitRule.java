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
