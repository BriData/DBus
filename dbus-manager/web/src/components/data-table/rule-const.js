const RuleConst = {
    type: {
        flattenUms: 'flattenUms',
        keyFilter: 'keyFilter',
        toIndex: 'toIndex',
        filter: 'filter',
        trim: 'trim',
        replace: 'replace',
        split: 'split',
        subString: 'subString',
        concat: 'concat',
        select: 'select',
        saveAs: 'saveAs',
        prefixOrAppend: 'prefixOrAppend',
        regexExtract: 'regexExtract'
    },
    rule: {
        groupId: 'groupId',
        orderId: 'orderId',
        ruleTypeName: 'ruleTypeName',
        ruleGrammar: 'ruleGrammar',
        ruleOperate: 'ruleOperate',
        ruleScope: 'ruleScope',
        ruleParamter: 'ruleParamter',
        name: 'name',
        type: 'type',
        ruleType: 'ruleType',
        subStartType: 'subStartType',
        subEndType: 'subEndType',
        subStart: 'subStart',
        subEnd: 'subEnd',
        filterKey: 'filterKey'
    },
    group: {
        tableId: 'tableId',
        groupName: 'groupName',
        status: 'status',
        updateTime: 'updateTime'
    },
    generateRuleGrammar: function() {
        return {
            ruleParamter: null,
            ruleOperate: null,
            ruleScope: null,
            name: null,
            type: null,
            ruleType: null,
            subStartType: null,
            subStart: null,
            subEndType: null,
            subEnd: null,
            filterKey: null
        };
    }
};

module.exports = RuleConst;
