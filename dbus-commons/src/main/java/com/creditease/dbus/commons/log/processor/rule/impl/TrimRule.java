package com.creditease.dbus.commons.log.processor.rule.impl;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.log.processor.parse.ParseResult;
import com.creditease.dbus.commons.log.processor.parse.ParseRuleGrammar;
import com.creditease.dbus.commons.log.processor.parse.RuleGrammar;
import com.creditease.dbus.commons.log.processor.rule.IRule;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedList;
import java.util.List;

public class TrimRule implements IRule {

    public List<String> transform(List<String> data, List<RuleGrammar> grammar, Rules ruleType) throws Exception{
        List<String> ret = new LinkedList<>(data);
        List<ParseResult> prList = ParseRuleGrammar.parse(grammar, data.size(), ruleType);
        for (ParseResult pr : prList) {
            for(int col : pr.getScope()) {
                if (col >= data.size()) continue;
                String value = data.get(col);
                ret.remove(col);
                if(StringUtils.equals(pr.getOperate(), Constants.TRIM_LEFT)) {
                    ret.add(col, StringUtils.stripStart(value, pr.getParamter()));
                } else if(StringUtils.equals(pr.getOperate(), Constants.TRIM_RIGHT)) {
                    ret.add(col, StringUtils.stripEnd(value, pr.getParamter()));
                } else if(StringUtils.equals(pr.getOperate(), Constants.TRIM_BOTH)) {
                    ret.add(col, StringUtils.strip(value, pr.getParamter()));
                } else {
                    ret.add(col, StringUtils.replaceChars(value, pr.getParamter(), ""));
                }
            }
        }
        return ret;
    }

    public static void main(String[] args) {
        String str = "aaaabbbccdd[]ggfz";
        String str2 = "abcdef17217abc2347912791018abc";
//        System.out.println(StringUtils.stripEnd("[12[[0]].00]", ".[]"));
        System.out.println(StringUtils.stripStart("2017-11-07T04:32:11.316Z", "2Z"));
//        System.out.println(StringUtils.strip("[12[[0]].00]", ".[]"));
    }
}
