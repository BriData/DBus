package com.creditease.dbus.commons.log.processor.rule.impl;

import com.creditease.dbus.commons.log.processor.parse.ParseResult;
import com.creditease.dbus.commons.log.processor.parse.ParseRuleGrammar;
import com.creditease.dbus.commons.log.processor.parse.RuleGrammar;
import com.creditease.dbus.commons.log.processor.rule.IRule;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexExtractRule implements IRule {

    public List<String> transform(List<String> data, List<RuleGrammar> grammar, Rules ruleType) throws Exception{
        List<String> ret = new LinkedList<>(data);
        List<ParseResult> prList = ParseRuleGrammar.parse(grammar, data.size(), ruleType);
        for (ParseResult pr : prList) {
            for (int col : pr.getScope()) {
                String value;
                if (col >= data.size()) {
                    value = "";
                } else {
                    value = data.get(col);
                    ret.remove(col);
                }
                Matcher matcher = Pattern.compile(pr.getParamter()).matcher(value);
                List<String> extractRet =  new ArrayList<>();
                if (matcher.find()) {
                    for (int i = 1; i <= matcher.groupCount(); i++) {
                        extractRet.add(matcher.group(i));
                    }
                    ret.addAll(col, extractRet);
                }
            }
        }
        return ret;
    }

    public static void main(String[] args) {
        String value = "2017年11月zl1313546";
        Matcher matcher = Pattern.compile("(\\d{4}年\\d{1,2}月)([a-z]+)\\d*").matcher(value);
        List<String> extractRet =  new ArrayList<>();

        if (matcher.find()) {
            System.out.println(matcher.group(1));
            System.out.println(matcher.group(2));
        }

        if (matcher.matches()) {
            for (int i=0; i<=matcher.groupCount(); i++) {
                extractRet.add(matcher.group(i));
            }

        }
        System.out.println(extractRet);
    }
}
