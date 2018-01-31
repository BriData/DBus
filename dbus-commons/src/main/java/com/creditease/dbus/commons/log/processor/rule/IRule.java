package com.creditease.dbus.commons.log.processor.rule;

import com.creditease.dbus.commons.log.processor.parse.RuleGrammar;
import com.creditease.dbus.commons.log.processor.rule.impl.Rules;

import java.util.List;

public interface IRule {

    public List<String> transform(List<String> data, List<RuleGrammar> grammar, Rules ruleType) throws Exception;

}
