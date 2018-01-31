package com.creditease.dbus.commons.log.processor.rule.impl;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.commons.log.processor.parse.ParseResult;
import com.creditease.dbus.commons.log.processor.parse.ParseRuleGrammar;
import com.creditease.dbus.commons.log.processor.parse.RuleGrammar;
import com.creditease.dbus.commons.log.processor.rule.IRule;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedList;
import java.util.List;

public class PrefixOrAppendRule implements IRule {

    public List<String> transform(List<String> data, List<RuleGrammar> grammar, Rules ruleType) throws Exception{
        List<String> ret = new LinkedList<>(data);
        List<ParseResult> prList = ParseRuleGrammar.parse(grammar, data.size(), ruleType);
        for (ParseResult pr : prList) {
            for (int col : pr.getScope()) {
                if (col >= data.size()) continue;
                String value = data.get(col);
                ret.remove(col);
                if (StringUtils.equals(pr.getOperate(), Constants.PREFIX_OR_APPEND_BEFORE)) {
                    ret.add(col, StringUtils.join(pr.getParamter(), value));
                } else {
                    ret.add(col, StringUtils.join(value, pr.getParamter()));
                }
            }
        }
        //返回结果
        return ret;
    }

    public static void main(String[] args) {
        System.out.println(StringUtils.join("aa", "bb"));
        System.out.println(StringUtils.join("bb", "aa"));
    }
}
