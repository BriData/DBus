package com.creditease.dbus.commons.log.processor.rule.impl;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.log.processor.parse.ParseResult;
import com.creditease.dbus.commons.log.processor.parse.ParseRuleGrammar;
import com.creditease.dbus.commons.log.processor.parse.RuleGrammar;
import com.creditease.dbus.commons.log.processor.rule.IRule;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;


public class FilterRule implements IRule {

    public List<String> transform(List<String> data, List<RuleGrammar> grammar, Rules ruleType) throws Exception{
        List<ParseResult> prList = ParseRuleGrammar.parse(grammar, data.size(), ruleType);
        boolean isOk = false;
        for (ParseResult pr : prList) {
            List<Integer> scope = pr.getScope();
            for (int col : scope) {
                String val;
                if (col >= data.size()) {
                    val = "";
                } else {
                    val = data.get(col);
                }
                if(StringUtils.equals(pr.getRuleType(), Constants.RULE_TYPE_STRING)) {
                    if (pr.getEq()) {
                        if (StringUtils.contains(val, pr.getParamter())) {
                            isOk = true;
                        }
                    } else {
                        if (!StringUtils.contains(val, pr.getParamter())) {
                            isOk = true;
                        }
                    }
                } else if(StringUtils.equals(pr.getRuleType(), Constants.RULE_TYPE_REGEX)) {
                    if(pr.getEq()) {
                        if(Pattern.compile(pr.getParamter()).matcher(val).find()) {
                            isOk = true;
                        }
                    } else {
                        if(!Pattern.compile(pr.getParamter()).matcher(val).find()) {
                            isOk = true;
                        }
                    }

                }
            }
        }
        return (isOk) ? data : new ArrayList<>();
    }


    public static void main(String[] args) {
        System.out.println(Pattern.compile("[A-Z]").matcher("ABCDEF").find());
        System.out.println(Pattern.compile("\\d{3}").matcher("T123aaa456").find());
    }
}
