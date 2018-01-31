package com.creditease.dbus.commons.log.processor.rule.impl;

import com.creditease.dbus.commons.log.processor.parse.ParseResult;
import com.creditease.dbus.commons.log.processor.parse.ParseRuleGrammar;
import com.creditease.dbus.commons.log.processor.parse.RuleGrammar;
import com.creditease.dbus.commons.log.processor.rule.IRule;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

public class ConcatRule implements IRule {

    public List<String> transform(List<String> data, List<RuleGrammar> grammar, Rules ruleType) throws Exception{
        List<String> ret = new LinkedList<>(data);
        List<ParseResult> prList = ParseRuleGrammar.parse(grammar, data.size(), ruleType);
        //将指定的列合并起来
        for (ParseResult pr : prList) {
            List<String> elems = new LinkedList<>();
            for (int col : pr.getScope()) {
                if (col >= data.size()) continue;
                elems.add(data.get(col));
            }
            ret.removeAll(elems);
            ret.add(pr.getScope().get(0), StringUtils.join(elems, pr.getParamter()));
        }
        //返回结果
        return ret;
    }

    public static void main(String[] args) {
        List<String> value = new LinkedList<>();
        List<String> value2 = new LinkedList<>();
        value.add("aa");
        value.add("bb");
        value.add("cc");
        value.add("dd");
        value2.add("aa");
        value2.add("aa");
        value.removeAll(value2);

        List<Integer> scope = new ArrayList<>();
        scope.add(0);
        scope.add(1);
        scope.add(2);
        scope.add(3);

        List<String> elems = new LinkedList<>();
        for (int col : scope) {
            elems.add(col, value.get(col));
        }
        System.out.println(StringUtils.join(elems, "#"));
    }
}
