package com.creditease.dbus.commons.log.processor.rule.impl;

import com.creditease.dbus.commons.log.processor.parse.ParseResult;
import com.creditease.dbus.commons.log.processor.parse.ParseRuleGrammar;
import com.creditease.dbus.commons.log.processor.parse.RuleGrammar;
import com.creditease.dbus.commons.log.processor.rule.IRule;

import java.util.ArrayList;
import java.util.List;

public class SelectRule implements IRule {

    public List<String> transform(List<String> data, List<RuleGrammar> grammar, Rules ruleType) throws Exception{
        List<String> ret = new ArrayList<>();
        List<ParseResult> prList = ParseRuleGrammar.parse(grammar, data.size(), ruleType);
        for (ParseResult pr : prList) {
            for (int col : pr.getScope()) {
                if (col >= data.size()) continue;
                ret.add(data.get(col));
            }
        }
        return ret;
    }

}
