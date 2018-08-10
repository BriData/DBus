/**
 * Created by haowei6 on 2017/10/11.
 */

var React = require('react');
var ReactDOM = require('react-dom');
var B = require('react-bootstrap');
var Select = require('../common/select');
var TF = require('../common/table/tab-frame');
var $ = require('jquery');
var utils = require('../common/utils');
var antd = require('antd');
const RuleConst = require('./rule-const');
var EditRuleGrammarLine = require('./edit-rule-grammar-line');

var EditRuleGrammar = React.createClass({

    getInitialState: function () {
        return {
            keyList: []
        };
    },

    generateKey: function () {
        var keyList = this.state.keyList;
        for (var i = 0; i < keyList.length; i++) {
            if (i != keyList[i]) {
                keyList.splice(i, 0, i);
                return i;
            }
        }
        var key = keyList.length;
        keyList.push(key);
        return key;
    },
    removeKey: function (key) {
        var keyList = this.state.keyList;
        for (var i = 0; i < keyList.length; i++) {
            if (key == keyList[i]) {
                keyList.splice(i, 1);
            }
        }
    },

    autoFillField: (rule, no) => {
        if (rule[RuleConst.rule.ruleTypeName] != RuleConst.type.jsonPath) return
        const ruleGrammars = rule[RuleConst.rule.ruleGrammar]
        if(!ruleGrammars || ruleGrammars.length < 2) return
        ruleGrammars[no + 1][RuleConst.rule.ruleScope] = ruleGrammars[no][RuleConst.rule.ruleScope]
        ruleGrammars[no + 1][RuleConst.rule.ruleParamter] = ruleGrammars[no][RuleConst.rule.ruleParamter]
    },

    arrangeToIndexOrder: function (rule) {
        if (rule[RuleConst.rule.ruleTypeName] != RuleConst.type.toIndex) return;
        var ruleGrammars = rule[RuleConst.rule.ruleGrammar];
        for (var i = 0; i < ruleGrammars.length; i++) {
            ruleGrammars[i][RuleConst.rule.ruleScope] = '' + i;
        }
    },

    handleAdd: function (no) {
        var rule = JSON.parse(JSON.stringify(this.props.rule));
        var ruleGrammars = rule[RuleConst.rule.ruleGrammar];
        ruleGrammars.splice(no + 1, 0, RuleConst.generateRuleGrammar());
        this.autoFillField(rule, no)
        this.arrangeToIndexOrder(rule);
        this.props.onRuleGrammarChange(rule);
    },
    handleDelete: function (no) {
        var rule = JSON.parse(JSON.stringify(this.props.rule));
        var ruleGrammars = rule[RuleConst.rule.ruleGrammar];
        if (ruleGrammars.length <= 1) return;
        this.removeKey(ruleGrammars[no].key);
        ruleGrammars.splice(no, 1);
        this.arrangeToIndexOrder(rule);
        this.props.onRuleGrammarChange(rule);
    },
    handleMoveUp: function (no) {
        if (no == 0) return;
        var rule = JSON.parse(JSON.stringify(this.props.rule));
        var ruleGrammars = rule[RuleConst.rule.ruleGrammar];
        [ruleGrammars[no], ruleGrammars[no - 1]] = [ruleGrammars[no - 1], ruleGrammars[no]];
        this.arrangeToIndexOrder(rule);
        this.props.onRuleGrammarChange(rule);
    },
    handleMoveDown: function (no) {
        var rule = JSON.parse(JSON.stringify(this.props.rule));
        var ruleGrammars = rule[RuleConst.rule.ruleGrammar];
        if (no == ruleGrammars.length - 1) return;
        [ruleGrammars[no], ruleGrammars[no + 1]] = [ruleGrammars[no + 1], ruleGrammars[no]];
        this.arrangeToIndexOrder(rule);
        this.props.onRuleGrammarChange(rule);
    },
    render: function () {

        var rule = this.props.rule;
        var ruleGrammars = rule[RuleConst.rule.ruleGrammar];
        var lines = [];
        for (let i = 0; i < ruleGrammars.length; i++) {
            let ruleGrammar = ruleGrammars[i];
            if(ruleGrammar.key == null) ruleGrammar.key = this.generateKey();
            lines.push(<EditRuleGrammarLine ruleGrammar={ruleGrammar}
                                            ruleType={rule[RuleConst.rule.ruleTypeName]}
                                            no={i}
                                            onAdd={this.handleAdd}
                                            onDelete={this.handleDelete}
                                            onMoveUp={this.handleMoveUp}
                                            onMoveDown={this.handleMoveDown}/>);
        }
        return (
            <div>
                {lines}
            </div>
        );
    }
});

module.exports = EditRuleGrammar;
